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
import java.util.Locale;
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
 * <p>Each unit returns its local candidate count, optionally enlarged by {@code oversample_factor};
 * merging them into the global top {@code k} is the caller's job - {@code
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
      LanceSearchQuery pinnedQuery =
          query.toBuilder()
              .topK(localCandidateK)
              .readOptions(
                  query
                      .getReadOptions()
                      .withRef(Utils.pinOpenedRef(dataset, query.getReadOptions().getRef())))
              .build();
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

      LanceSearchQuery resolvedQuery = pinnedQuery.toBuilder().vectorColumn(column).build();
      if ((resolvedQuery.getDistanceType() == null || resolvedQuery.getDistanceType().isEmpty())
          && vectorIndex.isPresent()
          && hasUncoveredFragments(existingFragments, vectorIndex.get())) {
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

      List<LanceDistributedSearchInputPartition> units =
          planUnits(resolvedQuery, existingFragments, vectorIndex, fastSearch);

      long indexedCount = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), globalCandidateK={}, localCandidateK={}",
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
      LanceSearchQuery resolvedQuery,
      Set<Integer> existingFragments,
      Optional<VectorIndexInfo> vectorIndex,
      boolean fastSearch) {
    List<LanceDistributedSearchInputPartition> units = new ArrayList<>();
    // Fragments still waiting for an owner; each index segment claims the ones it covers.
    Set<Integer> uncovered = new TreeSet<>(existingFragments);
    if (vectorIndex.isPresent()) {
      for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
        // A segment whose fragments are all gone (compaction, deletion) is stale: no unit for it.
        if (Collections.disjoint(segment.getFragmentIds(), existingFragments)) {
          continue;
        }
        uncovered.removeAll(segment.getFragmentIds());
        units.add(
            LanceDistributedSearchInputPartition.forIndexSegment(
                schema, resolvedQuery, segment.getUuid()));
      }
    }
    if (!fastSearch) {
      for (Integer fragmentId : uncovered) {
        units.add(
            LanceDistributedSearchInputPartition.forFragment(schema, resolvedQuery, fragmentId));
      }
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
        Set<Integer> fragmentIds = segment.fragments().map(HashSet::new).orElseGet(HashSet::new);
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

  private static boolean hasUncoveredFragments(
      Set<Integer> existingFragments, VectorIndexInfo vectorIndex) {
    Set<Integer> uncovered = new HashSet<>(existingFragments);
    for (VectorIndexSegment segment : vectorIndex.getSegments()) {
      uncovered.removeAll(segment.getFragmentIds());
    }
    return !uncovered.isEmpty();
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
    if (metric == null || metric.trim().isEmpty()) {
      return null;
    }
    switch (metric.toLowerCase(Locale.ROOT)) {
      case "l2":
      case "euclidean":
        return "l2";
      case "cosine":
        return "cosine";
      case "dot":
      case "ip":
      case "inner_product":
        return "dot";
      case "hamming":
        return "hamming";
      default:
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
    private final Set<Integer> fragmentIds;

    VectorIndexSegment(UUID uuid, Set<Integer> fragmentIds) {
      this.uuid = uuid;
      this.fragmentIds = Collections.unmodifiableSet(new HashSet<>(fragmentIds));
    }

    UUID getUuid() {
      return uuid;
    }

    Set<Integer> getFragmentIds() {
      return fragmentIds;
    }
  }
}
