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
 * <p>Each unit returns its own local top {@code k}; merging them into the global top {@code k} is
 * the caller's job - {@code LanceSearchTableFunctions} wraps this scan in a global sort and limit.
 * The merge sorts by the distance each unit reported, so a unit never needs to return more than
 * {@code k} rows: a row outside its local top {@code k} cannot enter the global top {@code k}.
 */
public class LanceDistributedSearchScan implements Scan, Batch, Serializable {
  private static final long serialVersionUID = -917364523098172364L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceDistributedSearchScan.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final String VECTOR_INDEX_DETAILS = "lance.index.pb.VectorIndexDetails";
  private static final String LEGACY_VECTOR_INDEX_DETAILS = "lance.index.VectorIndexDetails";

  /**
   * Column searched when {@code vector_column} is omitted. Must stay the field namespace execution
   * defaults to, as documented in docs/src/operations/dql/vector-search.md: picking the first
   * fixed-size-list field instead silently returns different neighbors on a table with more than
   * one vector column.
   */
  private static final String DEFAULT_VECTOR_COLUMN = "vector";

  private final StructType schema;
  private final LanceSearchQuery query;

  /**
   * Planned once per scan. Spark builds a fresh {@code BatchScanExec} for every materialization of
   * the same logical plan, so replanning would pin a new dataset version each time: two actions on
   * one DataFrame could then read different versions. Driver-only, hence transient.
   */
  private transient InputPartition[] plannedPartitions;

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
  public synchronized InputPartition[] planInputPartitions() {
    if (plannedPartitions == null) {
      plannedPartitions = planPartitionsOnce();
    }
    return plannedPartitions.clone();
  }

  private InputPartition[] planPartitionsOnce() {
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

      String column = resolveVectorColumn();
      boolean fastSearch = Boolean.TRUE.equals(query.getFastSearch());
      boolean bypassVectorIndex = Boolean.TRUE.equals(query.getBypassVectorIndex());
      Optional<VectorIndexInfo> vectorIndex =
          bypassVectorIndex
              ? Optional.empty()
              : selectVectorIndex(dataset, column, query.getDistanceType(), fastSearch);

      // Fragments no index segment covers; fast_search leaves them out entirely. A segment whose
      // fragments are all gone (compaction, deletion) is stale and gets no unit of its own.
      Set<Integer> fallbackFragments = new TreeSet<>(existingFragments);
      List<UUID> liveSegments = new ArrayList<>();
      if (vectorIndex.isPresent()) {
        for (VectorIndexSegment segment : vectorIndex.get().getSegments()) {
          fallbackFragments.removeAll(segment.getFragmentIds());
          if (!Collections.disjoint(segment.getFragmentIds(), existingFragments)) {
            liveSegments.add(segment.getUuid());
          }
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
      boolean mixedPlan = !liveSegments.isEmpty() && !fallbackFragments.isEmpty();
      // Indexed tasks search with the index's metric when none is given, while flat tasks would
      // default to L2; the global merge needs both on the same metric.
      if ((resolvedQuery.getDistanceType() == null || resolvedQuery.getDistanceType().isEmpty())
          && mixedPlan) {
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
      // The merge ranks every unit's rows against each other, so they all have to report distances
      // on one scale. A quantized index (IVF_PQ and friends) reports the quantized distance, while
      // a flat unit always reports the exact one. refine_factor makes an indexed unit take the
      // original vectors back and re-score, which is what lance-core itself does for this plan
      // shape - knn_combined re-scores exactly when a plan has uncovered fragments or stale rows,
      // and returns the ANN distances untouched otherwise. Mirror that: refine only a mixed plan,
      // so an all-indexed plan keeps reporting what the namespace path would report. A factor of 1
      // re-scores without enlarging the candidate set, since lance-core gates the re-score on the
      // factor being present rather than on its value.
      LanceSearchQuery indexedQuery =
          mixedPlan && resolvedQuery.getRefineFactor() == null
              ? resolvedQuery.toBuilder().refineFactor(1).build()
              : resolvedQuery;

      List<LanceDistributedSearchInputPartition> units =
          planUnits(indexedQuery, resolvedQuery, liveSegments, fallbackFragments);

      long indexedCount = units.stream().filter(u -> !u.getIndexSegments().isEmpty()).count();
      LOG.info(
          "Lance distributed vector search: column={}, indexName={}, units={} "
              + "(indexed={}, fallback={}), candidateK={}",
          column,
          vectorIndex.map(VectorIndexInfo::getIndexName).orElse("none"),
          units.size(),
          indexedCount,
          units.size() - indexedCount,
          resolvedQuery.getK());

      return units.toArray(new InputPartition[0]);
    } finally {
      dataset.close();
    }
  }

  private List<LanceDistributedSearchInputPartition> planUnits(
      LanceSearchQuery indexedQuery,
      LanceSearchQuery fallbackQuery,
      List<UUID> liveSegments,
      Set<Integer> fallbackFragments) {
    List<LanceDistributedSearchInputPartition> units = new ArrayList<>();
    for (UUID segmentUuid : liveSegments) {
      units.add(
          LanceDistributedSearchInputPartition.forIndexSegment(schema, indexedQuery, segmentUuid));
    }
    for (Integer fragmentId : fallbackFragments) {
      units.add(
          LanceDistributedSearchInputPartition.forFragment(schema, fallbackQuery, fragmentId));
    }
    return units;
  }

  private String resolveVectorColumn() {
    String declared = query.getVectorColumn();
    if (declared != null && !declared.isEmpty()) {
      return declared;
    }
    return DEFAULT_VECTOR_COLUMN;
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
      if (!indexMetricIsUsable(normalizedRequested, indexMetric)) {
        // Two different diagnoses, one decision. A real mismatch is a user error they can fix by
        // changing distance_type; an unreadable metric is a metadata problem no SQL change helps.
        if (indexMetric.isPresent()) {
          LOG.warn(
              "Ignoring vector index {} because query metric {} does not match index metric {}",
              idx.getName(),
              normalizedRequested,
              indexMetric.get());
        } else {
          LOG.warn(
              "Ignoring vector index {} because its metric could not be read from the index "
                  + "metadata, so it cannot be confirmed to match query metric {}",
              idx.getName(),
              normalizedRequested);
        }
        continue;
      }
      List<VectorIndexSegment> segments = new ArrayList<>();
      for (Index segment : idx.getSegments()) {
        segments.add(
            new VectorIndexSegment(
                segment.uuid(), requireFragmentCoverage(idx.getName(), segment.fragments())));
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

  /**
   * Splitting a search into indexed and flat units needs to know which fragments each index segment
   * covers. Indices written before Lance 0.8 did not persist that bitmap, and lance-core refuses
   * the same plan for the same reason - {@code Dataset::unindexed_fragments} in
   * rust/lance/src/index.rs returns "Please upgrade lance to 0.8+ to use this function" - so
   * namespace execution fails on these datasets too.
   */
  /**
   * Whether an index may answer a query asking for {@code requested}.
   *
   * <p>An omitted metric accepts any index - lance-core then searches with the index's own metric,
   * and {@code planPartitionsOnce} resolves it when the plan also has flat units. A requested
   * metric needs a resolved, equal index metric: unlike the namespace path, which opens the index
   * to read its metric and silently brute-forces on a mismatch, this planner can only read index
   * metadata, and handing an unverified segment to {@code indexSegments(...)} makes lance-core fail
   * the task outright rather than fall back. Losing the index is the safe direction.
   */
  static boolean indexMetricIsUsable(String requested, Optional<String> indexMetric) {
    if (requested == null) {
      return true;
    }
    return indexMetric.isPresent() && requested.equals(indexMetric.get());
  }

  static Set<Integer> requireFragmentCoverage(
      String indexName, Optional<List<Integer>> fragmentIds) {
    return fragmentIds
        .map(ids -> (Set<Integer>) new HashSet<>(ids))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Vector index '"
                        + indexName
                        + "' has a segment without a fragment bitmap; please upgrade the dataset "
                        + "to Lance 0.8+ to search it"));
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
