# Lance 12 升级影响说明

**升级范围**：`lance-core 11.0.0-beta.21 → 12.0.0-beta.18`（约 235 个提交）、`lance-namespace 0.8.6 → 0.11.1`（跨 0.9 / 0.11 两个次版本）。

两者必须一起升：`lance-core` 12.x 是对着 `lance-namespace` 0.11.1 编译的，jar 内自带的 `DirectoryNamespace` 引用了 0.8.6 里不存在的响应类型，继续 pin 0.8.6 会导致测试代码编译失败。

对应 PR：[lance-format/lance-spark#819](https://github.com/lance-format/lance-spark/pull/819)

---

## 一、影响最大：新建表的默认文件格式变成 2.2

来源：`feat: resolve stable file format to 2.2`（[lance#8657](https://github.com/lance-format/lance/pull/8657)）

Lance 的 `stable` 选择器和枚举默认值以前都停在 2.1，现在一起推到 2.2；`next` = 2.3。

### 具体影响

**1. 只影响新建的 dataset。**
`rust/lance/src/dataset/write/insert.rs:414-430` 的解析顺序是：

- append 到已有 dataset → 永远沿用该 dataset 的格式；
- overwrite 已有 dataset → 用户没指定时沿用现有格式；
- 全新 dataset → 才走默认值（现在是 2.2）。

所以存量表不会被悄悄升格。

**2. 读端必须是 Lance ≥ 10.0.0。**
2.2 的格式标识是 lance 10.0.0 的 `feat: introduce exact file format identity`（#7879）引入的。如果有 pylance / Daft / DuckDB 之类的旁路消费者锁在更老的 lance 上，会读不了新建的表。要保守就显式写：

```sql
TBLPROPERTIES ('file_format_version' = '2.1')
```

**3. legacy(v1) blob 列在 2.2 上被直接拒绝。**

```
Invalid user input: Legacy blob columns (field metadata key "lance-encoding:blob")
are not supported for file version >= 2.2. Found legacy blob field: data.
```

这条把 `<col>.lance.encoding = 'blob'` 这个用户接口打穿了 —— 不 pin 版本的建表会直接报错。

PR #819 的处理：请求 blob 编码但**没有** pin `file_format_version` 的表，自动创建在 `2.1`，保住文档承诺的 v1 行为；想要 blob v2 仍然要显式写 `2.2`。

> 备选方案（已否决）：让未 pin 版本的表跟随 lance 默认写 blob v2。这会把用户的读 schema 从 `BINARY` 变成 descriptor struct，虚拟列 `xxx__blob_pos` / `xxx__blob_size` 也会消失，对纯升级 PR 破坏太大。

**4. `stable` / `next` 现在解析到 2.2+。**
所以 `file_format_version = 'stable'` + blob 列会走 **blob v2**（读回来是 descriptor struct 而不是 `BINARY`），与升级前不同。PR #819 里 `BlobUtils.fileFormatSupportsBlobV2` 已识别这两个选择器，否则会先生成 v1 metadata、再在写入时被 lance 拒绝。

---

## 二、lance-namespace REST 契约破坏

来源：`feat!: add response context to REST spec`（[lance-namespace#358](https://github.com/lance-format/lance-namespace/pull/358)）

给全部 54 个操作的**响应**也加了 `context` 字段（对齐请求侧），并给 4 个原本不返回 JSON 对象的操作补了响应模型：

| 方法 | 旧签名 | 新签名 |
|---|---|---|
| `queryTable` | `byte[]` | `QueryTableResponse`（`context` + `data`） |
| `countTableRows` | `long` | `CountTableRowsResponse` |
| `namespaceExists` | `void` | `NamespaceExistsResponse` |
| `tableExists` | `void` | `TableExistsResponse` |

传输约定同时改了：`header.<name>` 前缀取代原来的 `x-lance-ctx-*`。REST 线上格式没变（仍是 Arrow 二进制 / 裸数字 / 200-404 状态码），变的是 Java / Python 接口。

### 对 lance-spark 的影响

- `LanceSearchColumnarPartitionReader` 里 `namespace.queryTable(...)` 需要 `.getData()` 解包（PR #819 已改）。
- **如果你自己实现了 `LanceNamespace`**（自定义 catalog），这四个方法的签名都得跟着改。

### 同区间其它 namespace 变更

- `feat: add tag field to DescribeTableRequest`（#345）
- `feat: add num_inserted_rows and version to InsertIntoTableResponse`（#359）
- `feat: declare and backfill expression-computed columns`（#360）
- `feat(spec): expose vector index build params on CreateTableIndexRequest`（#361）

后三条 lance-spark 目前都没调用，暂时无感。

---

## 三、`Update.updateMode` 变必填

来源：`feat(java): complete transaction operation mappings`（[lance#8925](https://github.com/lance-format/lance/pull/8925)）

Rust 事务模型允许 update mode 缺省，但 transaction protobuf 存不下"缺省"与 `RewriteRows` 的区别，所以 JNI 现在直接拒绝空 mode，而不是悄悄改变语义：

```
java.lang.IllegalArgumentException: update.updateMode must be specified because
the transaction format cannot persist an absent update mode distinctly from RewriteRows
```

**影响面**：lance-spark 所有 `UPDATE` / `DELETE` / `MERGE INTO` / CDF 相关的 60+ 个测试全部失败 —— 它们都走 `SparkPositionDeltaWrite` 的 commit 路径。

**修法**：在 `SparkPositionDeltaWrite`（3.4 与 3.5 各一份）显式写 `RewriteRows`，这本来就是缺省的含义，行为不变。

同一个 PR 还补齐了 `DataOverlay` / `UpdateMemWalState` / `Clone` / `UpdateBases` 的 Java 映射，并把无符号 id 溢出、非法 overlay 覆盖、非法 Bloom metadata 等从静默截断改成报错。

---

## 四、2.0 格式下写 null struct 从「静默损坏」改成报错

来源：`fix: reject lossy null structs in v2.0`（[lance#8507](https://github.com/lance-format/lance/pull/8507)）

以前 2.0 的 `CoreFieldEncodingStrategy` 不编码 struct validity，null struct 会被读回成「非 null 但子字段全 null 的 struct」——数据静默变形。现在写入直接失败：

```
Invalid user input: The struct field `address` contains 1 null value(s),
but Lance file version 2.0 does not encode struct validity; use file version 2.1 or later
```

**影响**：显式 pin 了 `2.0` 且 struct 列可空的表，升级后写入会失败。这是好事（以前是静默错误），但需要把 pin 改到 2.1+。

PR #819 里把 `testNullStructV2_0ReadsAsEmptyStruct` 改写成了 `testNullStructV2_0IsRejected`。

---

## 五、对象存储层整体换代

- `chore(deps): upgrade object_store to 0.14 and OpenDAL to 0.59`（[lance#9123](https://github.com/lance-format/lance/pull/9123)）
- `feat(io)!: paginated directory listing on ObjectStore`（#8606）
- `perf: push table listing pagination down to the object store`（#9165）
- `perf: check table deregistration status concurrently in list_directory_tables`（#9164）

**影响**：S3 / GCS / Azure / OSS 的行为和错误信息可能有细微变化；`dir` namespace 下 `SHOW TABLES` 改成分页下推，表多时会明显变快。

> ⚠️ 这块只在本地 `dir` namespace 下验证过，**S3 / Glue 路径未测**。升级前建议在自己的对象存储环境上跑一遍 CI 里的 Glue / S3 集成测试。

---

## 六、会改变查询 / DDL 结果的行为修复

| 变更 | 影响 |
|---|---|
| `fix: honor zero scanner limits`（#9111） | `LIMIT 0` 以前可能被当成「无限制」，现在真的返回 0 行 |
| `fix: make float filters treat -0.0 and 0.0 as the same value`（#6236） | 浮点列上 `WHERE x = 0.0` 现在也能匹配到 `-0.0` |
| `fix: refuse to drop a path that is not a Lance dataset`（#8665） | `DROP TABLE` 指向非 Lance 目录时报错，而不是把目录删掉 |
| `feat(cleanup): support cleaning specific dataset versions`（#8617） | 可以清理指定版本，不再只能按时间 |
| `fix(java): hold Dataset read lock when passing Dataset into native calls`（#8575） | 并发场景下的正确性修复 |
| 一批 `perf(fts)` / `perf(rowids)` / `perf(index)` | FTS 宽 AND 查询、稠密窗口打分、row id 探测均有加速，纯性能 |

---

## 七、新开的能力（lance-spark 目前还没用上）

不影响现有使用，但是后续工作的依赖基础：

- **`feat(java): support segment-based distributed vector search`（[lance#7169](https://github.com/lance-format/lance/pull/7169)）** —— 新增 `ScanOptions.indexSegments(List<UUID>)`，是 lance-spark PR #608「分布式 VECTOR_SEARCH」的硬依赖，**12.0.0-beta.17 才有**。
- `feat(java): expose merge insert write mode`（#8918）+ `feat(namespace)!: accept multiple columns for the merge insert on key`（#8915）—— lance-spark 的 MERGE INTO 走 Spark position-delta 路径，没调 lance 的 `merge_insert`，暂时无感。
- `feat(java): expose createIndex progress callbacks`（#8823）
- `feat(java): align inverted index options`（#8829）
- `feat(java): expose referenced file metadata`（#8962）
- `feat(index): split IVF partitions to target size and join undersized ones in one optimize pass`（#9051）
- `feat(index)!: default ivf_rq to 5-bit quantization`（#8936）—— lance-spark 的 `CREATE INDEX` SQL 目前只支持 BTREE / FTS / ZONEMAP 等标量索引，没有 IVF_RQ 入口，不受影响。

---

## 八、升级前自查清单

1. **有没有 pin 了 `file_format_version = '2.0'` 且含可空 struct 列的表？** → 写入会失败，改 pin 到 2.1+。
2. **有没有依赖 `<col>.lance.encoding = 'blob'` 且不 pin 版本的建表？** → PR #819 已兜住（自动 pin 2.1）；如果本来就想上 blob v2，正好显式改成 `2.2`。
3. **有没有旁路读 lance 文件的消费者锁在 lance < 10？** → 新建表默认 2.2 会读不了。
4. **有没有自己实现的 `LanceNamespace`？** → `queryTable` / `countTableRows` / `namespaceExists` / `tableExists` 四个方法签名要改。
5. **S3 / Glue / Azure 环境** → 对象存储层换代未在本地验证，跑一遍集成测试。

---

## 九、PR #819 的改动清单与验证结果

### 主代码

| 文件 | 改动 |
|---|---|
| `pom.xml` | `lance.version` → 12.0.0-beta.18；`lance-namespace.version` → 0.11.1 |
| `search/LanceSearchColumnarPartitionReader.java` | `queryTable(...).getData()` |
| `write/SparkPositionDeltaWrite.java`（3.4 / 3.5） | 显式 `.updateMode(Optional.of(Update.UpdateMode.RewriteRows))` |
| `CreateTableSpec.java` | 未 pin 版本 + 请求 blob 编码 → pin 到 `2.1` |
| `utils/BlobUtils.java` | 新增 `MAX_BLOB_V1_FILE_FORMAT_VERSION`、`requestsBlobEncoding()`；`fileFormatSupportsBlobV2()` 识别 `stable` / `next` |
| `utils/SchemaConverter.java` | 复用 `BlobUtils` 的属性常量 |

### 测试与文档

- `BaseLanceFormatTest`：`testNullStructV2_0ReadsAsEmptyStruct` → `testNullStructV2_0IsRejected`
- `BaseSparkConnectorWriteTest`：`STABLE` 断言不再硬编码 `2.0`/`2.1`
- `CreateTableSpecTest` / `SchemaConverterTest` / `BlobUtilsTest`：更新命名选择器与未 pin 版本的预期，新增 pin-to-2.1 用例
- `LanceRuntimeQueryTableSupportTest` / `BaseFtsCatalogOnlyNamespaceTest`：namespace 接口签名
- `docs/src/config.md`、`docs/src/operations/ddl/create-table.md`：blob 版本解析规则

### 验证

| 模块 | 结果 |
|---|---|
| `lance-spark-3.5_2.12` | 1400 tests，0 failures / 0 errors |
| `lance-spark-3.4_2.12` | 1253 tests，0 failures / 0 errors |
| `lance-spark-4.2_2.13` | 1456 tests，0 failures / 0 errors |
| 全 reactor（16 模块） | `test-compile` 通过 |
| 代码风格 | `spotless:check` 干净 |

未覆盖：S3 / Glue / Azure 等真实对象存储路径；Spark 4.0 / 4.1 完整测试（仅 test-compile）。
