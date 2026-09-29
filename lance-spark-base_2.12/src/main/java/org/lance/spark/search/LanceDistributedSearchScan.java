/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.search;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.lance.spark.utils.Utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Plans a vector search as one Spark task per searchable unit of the dataset: one task per segment
 * of the vector index, plus one flat-KNN task per fragment no segment covers (unless {@code
 * fast_search} asked for indexed data only).
 *
 * <p>Each unit returns its local top {@code k}, which {@code oversample_factor} enlarges for
 * indexed units; merging them into the global top {@code k} is the caller's job - {@code
 * LanceSearchTableFunctions} wraps this scan in a global sort and limit.
 */
public class LanceDistributedSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -917364523098172364L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceDistributedSearchScan.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final String VECTOR_INDEX_DETAILS = "lance.index.pb.VectorIndexDetails";
  private static final String LEGACY_VECTOR_INDEX_DETAILS = "lance.index.VectorIndexDetails";

  private final StructType schema;
  private final LanceSearchQuery query;

  public LanceDistributedSearchScan(StructType schema, LanceSearchQuery query) {
    Objects.requireNonNull(
        query.getReadOptions(), "query.readOptions is required for distributed search");
    this.schema = schema;
    this.query = query;
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public String description() {
    return "LanceDistributedSearchScan";
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceDistributedSearchPartitionReaderFactory();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    int globalCandidateK = query.getK();
    int localCandidateK = localCandidateK(globalCandidateK, query.getOversampleFactor());
    Dataset dataset =
        Utils.openDatasetBuilder(query.getReadOptions())
            .initialStorageOptions(query.getInitialStorageOptions())
            .build();
    try {
      Set<Integer> existingFragments = new HashSet<>();
      for (Fragment fragment : dataset.getFragments()) {
        existingFragments.add(fragment.getId());
      }
      if (existingFragments.isEmpty()) {
        LOG.info("Lance distributed vector search: empty dataset, returning empty result");
        return new InputPartition[0];
      }

      String column = resolveVectorColumn(dataset);
      boolean fastSearch = Boolean.TRUE.equals(query.getFastSearch());
      boolean bypassVectorIndex = Boolean.TRUE.equals(query.getBypassVectorIndex());
      Optional<VectorIndexInfo> vectorIndex =
          bypassVectorIndex
              ? Optional.empty()
              : selectVectorIndex(dataset, column, query.getDistanceType(), fastSearch);

      // Fragments no index segment covers; fast_search leaves them out entirely.
      Set<Integer> fallbackFragments = new TreeSet<>(existingFragments);
      List<Optional<Set<Integer>>> indexSegmentCoverages = new ArrayList<>();
      if (vectorIndex.isPresent()) {
        for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
          indexSegmentCoverages.add(segment.getFragmentIds());
          segment.getFragmentIds().ifPresent(fallbackFragments::removeAll);
        }
      }
      if (fastSearch) {
        fallbackFragments.clear();
      }

      LanceSearchQuery resolvedQuery =
          query.toBuilder()
              .vectorColumn(column)
              .readOptions(
                  query
                      .getReadOptions()
                      .withRef(Utils.pinOpenedRef(dataset, query.getReadOptions().getRef())))
              .build();
      // Indexed tasks search with the index's metric when none is given, while flat tasks would
      // default to L2; the global merge needs both on the same metric.
      if ((resolvedQuery.getDistanceType() == null || resolvedQuery.getDistanceType().isEmpty())
          && shouldResolveIndexMetricForFallback(
              indexSegmentCoverages, existingFragments, fallbackFragments)) {
        String indexMetric =
            vectorIndex
                .get()
                .getMetric()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Cannot determine the distance metric for vector index '"
                                + vectorIndex.get().getIndexName()
                                + "'; pass distance_type explicitly before mixing indexed and "
                                + "unindexed fragments"));
        resolvedQuery = resolvedQuery.toBuilder().distanceType(indexMetric).build();
      }
      // Oversampling only helps approximate index searches: a flat task already returns its
      // exact local top k.
      LanceSearchQuery indexedQuery = resolvedQuery.toBuilder().topK(localCandidateK).build();

      List<LanceDistributedSearchInputPartition> units =
          planUnits(
              indexedQuery,
              resolvedQuery,
              existingFragments,
              vectorIndex,
              fallbackFragments,
              fastSearch);

      long indexedCount = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), globalCandidateK={}, indexedCandidateK={}",
          column,
          vectorIndex.map(VectorIndexInfo::getIndexName).orElse("none"),
          units.size(),
          indexedCount,
          units.size() - indexedCount,
          globalCandidateK,
          localCandidateK);

      return units.toArray(new InputPartition[0]);
    } finally {
      dataset.close();
    }
  }

  private List<LanceDistributedSearchInputPartition> planUnits(
      LanceSearchQuery indexedQuery,
      LanceSearchQuery fallbackQuery,
      Set<Integer> existingFragments,
      Optional<VectorIndexInfo> vectorIndex,
      Set<Integer> fallbackFragments,
      boolean fastSearch) {
    List<LanceDistributedSearchInputPartition> units = new ArrayList<>();
    if (vectorIndex.isPresent()) {
      for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
        if (!shouldPlanIndexSegment(segment.getFragmentIds(), existingFragments, fastSearch)) {
          continue;
        }
        units.add(
            LanceDistributedSearchInputPartition.forIndexSegment(
                schema, indexedQuery, segment.getUuid()));
      }
    }
    for (Integer fragmentId : fallbackFragments) {
      units.add(
          LanceDistributedSearchInputPartition.forFragment(schema, fallbackQuery, fragmentId));
    }
    return units;
  }

  private String resolveVectorColumn(Dataset dataset) {
    String declared = query.getVectorColumn();
    if (declared != null && !declared.isEmpty()) {
      return declared;
    }
    for (LanceField field : dataset.getLanceSchema().fields()) {
      if (field.getType() instanceof ArrowType.FixedSizeList) {
        return field.getName();
      }
    }
    throw new IllegalArgumentException(
        "VECTOR_SEARCH could not auto-detect a vector column; pass vector_column explicitly");
  }

  private static Optional<VectorIndexInfo> selectVectorIndex(
      Dataset dataset, String column, String requestedMetric, boolean fastSearch) {
    List<IndexDescription> indices;
    try {
      indices = dataset.describeIndices(new IndexCriteria.Builder().build());
    } catch (Exception e) {
      failFastSearchWhenIndexDiscoveryFails(fastSearch, e);
      LOG.warn("describeIndices failed, falling back to flat search: {}", e.getMessage());
      return Optional.empty();
    }

    Map<Integer, String> fieldIdToName = new HashMap<>();
    for (LanceField field : dataset.getLanceSchema().fields()) {
      fieldIdToName.put(field.getId(), field.getName());
    }

    String normalizedRequested = normalizeMetric(requestedMetric);
    boolean sawIndexForColumn = false;
    for (IndexDescription idx : indices) {
      if (!isVectorIndex(idx)) {
        continue;
      }
      if (idx.getFieldIds().isEmpty()
          || !column.equals(fieldIdToName.get(idx.getFieldIds().get(0)))) {
        continue;
      }
      sawIndexForColumn = true;
      Optional<String> indexMetric = resolveIndexMetric(dataset, idx);
      if (normalizedRequested != null
          && (!indexMetric.isPresent() || !normalizedRequested.equals(indexMetric.get()))) {
        LOG.info(
            "Ignoring vector index {} because query metric {} does not match index metric {}",
            idx.getName(),
            normalizedRequested,
            indexMetric.orElse("unknown"));
        continue;
      }
      List<VectorIndexSegment> segments = new ArrayList<>();
      for (Index segment : idx.getSegments()) {
        UUID uuid = segment.uuid();
        Optional<Set<Integer>> fragmentIds = segment.fragments().map(HashSet::new);
        segments.add(new VectorIndexSegment(uuid, fragmentIds));
      }
      return Optional.of(new VectorIndexInfo(idx.getName(), segments, indexMetric));
    }
    if (fastSearch && normalizedRequested != null && sawIndexForColumn) {
      throw new IllegalArgumentException(
          "fast_search cannot use any vector index on column '"
              + column
              + "' with distance_type '"
              + normalizedRequested
              + "'");
    }
    return Optional.empty();
  }

  private static Optional<String> resolveIndexMetric(
      Dataset dataset, IndexDescription description) {
    String details = description.getDetailsJson();
    if (details != null && !details.trim().isEmpty()) {
      try {
        JsonNode metric = JSON_MAPPER.readTree(details).get("metric_type");
        if (metric != null && metric.isTextual()) {
          String normalized = normalizeMetric(metric.asText());
          if (normalized != null) {
            return Optional.of(normalized);
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "Could not parse details for vector index {}: {}",
            description.getName(),
            e.getMessage());
      }
    }

    try {
      Set<String> metrics = indexMetricTypes(dataset.getIndexStatistics(description.getName()));
      if (metrics.size() == 1) {
        return Optional.of(metrics.iterator().next());
      }
    } catch (Exception e) {
      LOG.warn(
          "Could not read statistics for vector index {}: {}",
          description.getName(),
          e.getMessage());
    }
    return Optional.empty();
  }

  static Set<String> indexMetricTypes(Map<String, Object> statistics) {
    Set<String> metrics = new HashSet<>();
    Object indices = statistics.get("indices");
    if (!(indices instanceof Iterable)) {
      return metrics;
    }
    for (Object item : (Iterable<?>) indices) {
      if (!(item instanceof Map)) {
        continue;
      }
      Object metric = ((Map<?, ?>) item).get("metric_type");
      String normalized = metric == null ? null : normalizeMetric(String.valueOf(metric));
      if (normalized != null) {
        metrics.add(normalized);
      }
    }
    return metrics;
  }

  private static String normalizeMetric(String metric) {
    try {
      return LanceSearchQuery.canonicalizeDistanceType(metric);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  static int localCandidateK(int globalCandidateK, Float oversampleFactor) {
    float factor = oversampleFactor == null ? 1.0f : oversampleFactor;
    if (!Float.isFinite(factor) || factor < 1.0f) {
      throw new IllegalArgumentException("oversample_factor must be finite and at least 1.0");
    }
    double candidateK = Math.ceil(globalCandidateK * (double) factor);
    if (candidateK > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "oversample_factor produces more than " + Integer.MAX_VALUE + " candidates per task");
    }
    return (int) candidateK;
  }

  static boolean shouldPlanIndexSegment(
      Optional<Set<Integer>> fragmentIds, Set<Integer> existingFragments, boolean fastSearch) {
    if (!fragmentIds.isPresent()) {
      // Legacy index segments did not persist their fragment bitmap.  A normal search falls back
      // to all live fragments because it cannot safely determine which fragments are unindexed.
      // fast_search explicitly excludes fallback work, so it must still search the legacy segment
      // instead of mistaking unknown coverage for an empty/stale segment.
      return fastSearch;
    }
    return !Collections.disjoint(fragmentIds.get(), existingFragments);
  }

  static boolean shouldResolveIndexMetricForFallback(
      List<Optional<Set<Integer>>> segmentCoverages,
      Set<Integer> existingFragments,
      Set<Integer> fallbackFragments) {
    if (fallbackFragments.isEmpty()) {
      return false;
    }
    // A known live segment will be searched alongside the fallback fragments. Missing coverage is
    // a legacy index: normal mode uses complete flat fallback, but preserving the index metric
    // keeps the implicit distance_type consistent with namespace execution.
    for (Optional<Set<Integer>> coverage : segmentCoverages) {
      if (shouldPlanIndexSegment(coverage, existingFragments, true)) {
        return true;
      }
    }
    return false;
  }

  static void failFastSearchWhenIndexDiscoveryFails(boolean fastSearch, Exception cause) {
    if (fastSearch) {
      throw new IllegalStateException(
          "fast_search cannot continue because vector index discovery failed", cause);
    }
  }

  static boolean isVectorIndexTypeUrl(String typeUrl) {
    if (typeUrl == null) {
      return false;
    }
    int separator = typeUrl.lastIndexOf('/');
    if (separator < 0 || separator == typeUrl.length() - 1) {
      return false;
    }
    String detailsType = typeUrl.substring(separator + 1);
    return VECTOR_INDEX_DETAILS.equalsIgnoreCase(detailsType)
        || LEGACY_VECTOR_INDEX_DETAILS.equalsIgnoreCase(detailsType);
  }

  private static boolean isVectorIndex(IndexDescription idx) {
    return isVectorIndexTypeUrl(idx.getTypeUrl());
  }

  /** Lightweight view of a vector index for planning. */
  private static final class VectorIndexInfo {
    private final String indexName;
    private final List<VectorIndexSegment> segments;
    private final Optional<String> metric;

    VectorIndexInfo(String indexName, List<VectorIndexSegment> segments, Optional<String> metric) {
      this.indexName = indexName;
      this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
      this.metric = metric;
    }

    String getIndexName() {
      return indexName;
    }

    List<VectorIndexSegment> getSegments() {
      return segments;
    }

    Optional<String> getMetric() {
      return metric;
    }
  }

  /** One physical segment of a vector index. */
  private static final class VectorIndexSegment {
    private final UUID uuid;
    private final Optional<Set<Integer>> fragmentIds;

    VectorIndexSegment(UUID uuid, Optional<Set<Integer>> fragmentIds) {
      this.uuid = uuid;
      this.fragmentIds = fragmentIds.map(ids -> Collections.unmodifiableSet(new HashSet<>(ids)));
    }

    UUID getUuid() {
      return uuid;
    }

    Optional<Set<Integer>> getFragmentIds() {
      return fragmentIds;
    }
  }
}
