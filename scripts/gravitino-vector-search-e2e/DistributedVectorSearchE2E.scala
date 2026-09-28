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

import java.util.{ArrayList, Collections}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.lance.Dataset
import org.lance.index.{DistanceType, Index, IndexOptions, IndexParams, IndexType}
import org.lance.index.vector.{IvfBuildParams, VectorIndexParams, VectorTrainer}
import org.lance.spark.LanceDataset
import org.lance.spark.search.{LanceDistributedSearchInputPartition, LanceDistributedSearchScan, LanceSearchQuery}
import org.lance.spark.search.LanceSearchQuery.SearchType
import org.lance.spark.utils.Utils

spark.sparkContext.setLogLevel("WARN")

val mode = sys.env.getOrElse("E2E_MODE", "verify")
val runId = sys.env.getOrElse("E2E_RUN_ID", throw new IllegalArgumentException("E2E_RUN_ID is required"))
val namespace = sys.env.getOrElse("E2E_NAMESPACE", "vs")
val prefix = sys.env.getOrElse("E2E_TABLE_PREFIX", s"dvs_e2e_$runId")
val dim = 8
val queryVectorSql = (0 until dim).map(_ => "CAST(0.0 AS FLOAT)").mkString("array(", ", ", ")")
val queryVector = (0 until dim).map(_ => Float.box(0.0f)).asJava
val fallbackName = s"${prefix}_fallback"
val indexedName = s"${prefix}_indexed"
val mixedName = s"${prefix}_mixed"
val fallbackTable = s"lance.$namespace.$fallbackName"
val indexedTable = s"lance.$namespace.$indexedName"
val mixedTable = s"lance.$namespace.$mixedName"
val failures = ArrayBuffer.empty[String]

def requireE2E(condition: Boolean, message: String): Unit = {
  if (!condition) throw new IllegalStateException(message)
}

def throwableText(error: Throwable): String = {
  Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map { current =>
    s"${current.getClass.getSimpleName}: ${Option(current.getMessage).getOrElse("")}".replace('\n', ' ')
  }.mkString(" | ")
}

def check(name: String)(body: => Unit): Unit = {
  val started = System.currentTimeMillis()
  try {
    body
    println(s"E2E|case=$name|status=PASS|ms=${System.currentTimeMillis() - started}")
  } catch {
    case NonFatal(error) =>
      val detail = throwableText(error).take(1000)
      failures += s"$name: $detail"
      println(s"E2E|case=$name|status=FAIL|ms=${System.currentTimeMillis() - started}|detail=$detail")
  }
}

def table(tableName: String): LanceDataset = {
  val catalog = spark.sessionState.catalogManager.catalog("lance").asInstanceOf[TableCatalog]
  catalog.loadTable(Identifier.of(Array(namespace), tableName)).asInstanceOf[LanceDataset]
}

def openDataset(tableName: String): Dataset = {
  val lanceTable = table(tableName)
  Utils.openDatasetBuilder(lanceTable.readOptions())
    .initialStorageOptions(lanceTable.getInitialStorageOptions())
    .build()
}

def fullName(tableName: String): String = s"lance.$namespace.$tableName"

def createTable(tableName: String, fragmentCount: Int): Unit = {
  val name = fullName(tableName)
  spark.sql(
    s"""CREATE TABLE $name (
       |  id INT NOT NULL,
       |  vector ARRAY<FLOAT> NOT NULL
       |) USING lance
       |TBLPROPERTIES ('vector.arrow.fixed-size-list.size' = '$dim')
       |""".stripMargin)
  for (fragment <- 0 until fragmentCount) {
    val start = fragment * 64
    val end = start + 64
    spark.sql(
      s"""INSERT INTO $name
         |SELECT CAST(id AS INT), transform(sequence(1, $dim), x -> CAST(id + 10 AS FLOAT))
         |FROM range($start, $end, 1, 1)
         |""".stripMargin)
  }
  spark.sql(s"REFRESH TABLE $name")
}

def createSegmentIndex(tableName: String, indexName: String): Int = {
  val dataset = openDataset(tableName)
  try {
    val fragments = dataset.getFragments().asScala.map(_.getId()).sorted
    val trainParams = new IvfBuildParams.Builder().setNumPartitions(2).setMaxIters(2).build()
    val centroids = VectorTrainer.trainIvfCentroids(dataset, "vector", trainParams)
    val ivfParams = new IvfBuildParams.Builder()
      .setNumPartitions(2)
      .setMaxIters(2)
      .setCentroids(centroids)
      .build()
    val params = IndexParams.builder()
      .setVectorIndexParams(
        new VectorIndexParams.Builder(ivfParams).setDistanceType(DistanceType.L2).build())
      .build()
    val segments = new ArrayList[Index]()
    fragments.foreach { fragmentId =>
      segments.add(dataset.createIndex(
        IndexOptions.builder(Collections.singletonList("vector"), IndexType.IVF_FLAT, params)
          .withIndexName(indexName)
          .withFragmentIds(Collections.singletonList(Integer.valueOf(fragmentId)))
          .build()))
    }
    val committed = dataset.commitExistingIndexSegments(indexName, "vector", segments)
    requireE2E(committed.size() == fragments.size, s"expected ${fragments.size} segments, got ${committed.size()}")
    fragments.size
  } finally dataset.close()
}

def plan(tableName: String, fastSearch: java.lang.Boolean = null, filter: String = null): Seq[LanceDistributedSearchInputPartition] = {
  val lanceTable = table(tableName)
  val query = LanceSearchQuery.builder(SearchType.VECTOR)
    .tableId(lanceTable.readOptions().getTableId())
    .namespaceImpl(lanceTable.getNamespaceImpl())
    .namespaceProperties(lanceTable.getNamespaceProperties())
    .readOptions(lanceTable.readOptions())
    .initialStorageOptions(lanceTable.getInitialStorageOptions())
    .outputColumns(Collections.singletonList("id"))
    .vector(queryVector)
    .topK(5)
    .vectorColumn("vector")
    .nprobes(2)
    .filter(filter)
    .fastSearch(fastSearch)
    .build()
  new LanceDistributedSearchScan(lanceTable.schema(), query)
    .planInputPartitions()
    .toSeq
    .map(_.asInstanceOf[LanceDistributedSearchInputPartition])
}

def planCounts(partitions: Seq[LanceDistributedSearchInputPartition]): (Int, Int) = {
  val indexed = partitions.count(partition => !partition.getIndexSegments().isEmpty())
  (indexed, partitions.size - indexed)
}

def exactIds(tableName: String, k: Int, filter: String = null, offset: Int = 0): Seq[Int] = {
  val where = Option(filter).map(value => s"WHERE $value").getOrElse("")
  spark.sql(
    s"""SELECT id
       |FROM (
       |  SELECT id,
       |         aggregate(transform(vector, value -> CAST(value AS DOUBLE) * CAST(value AS DOUBLE)),
       |                   CAST(0.0 AS DOUBLE), (acc, value) -> acc + value) AS exact_distance
       |  FROM ${fullName(tableName)}
       |  $where
       |)
       |ORDER BY exact_distance, id
       |LIMIT $k OFFSET $offset
       |""".stripMargin).collect().map(_.getInt(0)).toSeq
}

def searchIds(
    tableName: String,
    k: Int,
    fastSearch: java.lang.Boolean = null,
    filter: String = null,
    offset: Int = 0): Seq[Int] = {
  val fastArg = Option(fastSearch).map(value => s", fast_search => ${value.booleanValue()}").getOrElse("")
  val filterArg = Option(filter).map(value => s", filter => '$value'").getOrElse("")
  val offsetArg = if (offset > 0) s", offset => $offset" else ""
  spark.sql(
    s"""SELECT id, _distance
       |FROM VECTOR_SEARCH(
       |  table => '${fullName(tableName)}',
       |  query_vector => $queryVectorSql,
       |  vector_column => 'vector',
       |  columns => array('id'),
       |  num_results => $k,
       |  nprobes => 2$fastArg$filterArg$offsetArg)
       |ORDER BY _distance, id
       |""".stripMargin).collect().map(_.getInt(0)).toSeq
}

def verifyFallback(): Seq[Int] = {
  val counts = planCounts(plan(fallbackName))
  requireE2E(counts == (0, 4), s"fallback plan expected (0,4), got $counts")
  val expected = exactIds(fallbackName, 5)
  val actual = searchIds(fallbackName, 5)
  requireE2E(actual == expected, s"fallback expected=$expected actual=$actual")
  println(s"RESULT|fallback_tasks=${counts._2}|fallback_topk=${actual.mkString(",")}")
  actual
}

def verifyCredentialRefresh(): Unit = {
  val counts = planCounts(plan(fallbackName))
  requireE2E(counts == (0, 4), s"credential plan expected (0,4), got $counts")
  val actual = searchIds(fallbackName, 5)
  requireE2E(actual == Seq(0, 1, 2, 3, 4), s"credential query got unexpected result: $actual")
  println(s"RESULT|fallback_tasks=${counts._2}|fallback_topk=${actual.mkString(",")}")
}

def verifyIndexed(): Seq[Int] = {
  val counts = planCounts(plan(indexedName))
  requireE2E(counts == (4, 0), s"indexed plan expected (4,0), got $counts")
  val expected = exactIds(indexedName, 5)
  val actual = searchIds(indexedName, 5)
  requireE2E(actual == expected, s"indexed expected=$expected actual=$actual")
  println(s"RESULT|indexed_tasks=${counts._1}|indexed_topk=${actual.mkString(",")}")
  actual
}

def verifyMixedAndFast(): Unit = {
  val counts = planCounts(plan(mixedName))
  requireE2E(counts == (3, 1), s"mixed plan expected (3,1), got $counts")
  val expected = exactIds(mixedName, 5)
  val actual = searchIds(mixedName, 5)
  requireE2E(actual == expected && actual.head == 999, s"mixed expected=$expected actual=$actual")

  val fastCounts = planCounts(plan(mixedName, java.lang.Boolean.TRUE))
  val fast = searchIds(mixedName, 5, java.lang.Boolean.TRUE)
  requireE2E(fastCounts == (3, 0), s"fast plan expected (3,0), got $fastCounts")
  requireE2E(!fast.contains(999), s"fast search included unindexed id 999: $fast")
  println(
    s"RESULT|mixed_indexed=${counts._1}|mixed_fallback=${counts._2}|mixed_topk=${actual.mkString(",")}|fast_topk=${fast.mkString(",")}")
}

def verifyOptions(): Unit = {
  val filter = "id % 2 = 0"
  val filteredExpected = exactIds(fallbackName, 5, filter)
  val filtered = searchIds(fallbackName, 5, null, filter)
  requireE2E(filtered == filteredExpected, s"filter expected=$filteredExpected actual=$filtered")

  val offsetExpected = exactIds(fallbackName, 3, null, 1)
  val offset = searchIds(fallbackName, 3, null, null, 1)
  requireE2E(offset == offsetExpected, s"offset expected=$offsetExpected actual=$offset")

  val oversized = searchIds(fallbackName, 300)
  requireE2E(oversized.size == 256, s"k > rows expected 256 rows, got ${oversized.size}")
  requireE2E(oversized == exactIds(fallbackName, 300), "k > rows result mismatch")
  println(
    s"RESULT|filter_topk=${filtered.mkString(",")}|offset_topk=${offset.mkString(",")}|oversized_k_rows=${oversized.size}")
}

def verifyNoSparkStorageConfig(): Unit = {
  val storageKeys = spark.conf.getAll.keys.filter(_.startsWith("spark.sql.catalog.lance.storage.")).toSeq
  requireE2E(storageKeys.isEmpty, s"Spark-side storage config is forbidden: ${storageKeys.sorted.mkString(",")}")
}

println(s"E2E|mode=$mode|run_id=$runId|master=${spark.sparkContext.master}|prefix=$prefix")
check("no-spark-storage-config") { verifyNoSparkStorageConfig() }

mode match {
  case "setup" =>
    check("create-fallback-table") { createTable(fallbackName, 4) }
    check("create-indexed-table") {
      createTable(indexedName, 4)
      requireE2E(createSegmentIndex(indexedName, s"${prefix}_ivf4") == 4, "indexed table segment count mismatch")
      spark.sql(s"REFRESH TABLE $indexedTable")
    }
    check("create-mixed-table") {
      createTable(mixedName, 3)
      requireE2E(createSegmentIndex(mixedName, s"${prefix}_ivf3") == 3, "mixed table segment count mismatch")
      spark.sql(
        s"""INSERT INTO $mixedTable VALUES
           |(999, $queryVectorSql)
           |""".stripMargin)
      spark.sql(s"REFRESH TABLE $mixedTable")
    }
    check("distributed-disabled-query-404") {
      spark.conf.set("spark.sql.lance.search.distributed.enabled", "false")
      var failure: Throwable = null
      try searchIds(fallbackName, 5) catch { case NonFatal(error) => failure = error }
      spark.conf.set("spark.sql.lance.search.distributed.enabled", "true")
      requireE2E(failure != null, "distributed.enabled=false unexpectedly succeeded")
      requireE2E(throwableText(failure).contains("404"), s"expected query 404, got ${throwableText(failure)}")
    }
    check("setup-fallback") { verifyFallback() }
    check("setup-indexed") { verifyIndexed() }
    check("setup-mixed-fast") { verifyMixedAndFast() }
    check("setup-options") { verifyOptions() }

  case "verify" =>
    check("verify-fallback") { verifyFallback() }
    check("verify-indexed") { verifyIndexed() }
    check("verify-mixed-fast") { verifyMixedAndFast() }
    check("verify-options") { verifyOptions() }

  case "credentials" =>
    check("credential-fallback-plan-and-query") { verifyCredentialRefresh() }

  case "cleanup" =>
    check("drop-test-tables") {
      Seq(fallbackTable, indexedTable, mixedTable).foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name PURGE")
      }
    }

  case other =>
    failures += s"unsupported E2E_MODE=$other"
}

println(s"E2E|summary|mode=$mode|failures=${failures.size}")
failures.foreach(failure => println(s"E2E|failure=$failure"))
spark.stop()
System.exit(if (failures.isEmpty) 0 else 1)
