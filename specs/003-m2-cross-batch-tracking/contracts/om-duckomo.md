# M2 OM 样本转换与 DuckOMO 读取契约

版本：`m2.om.v1`。对应 FR-002、FR-013–FR-015、SC-001–SC-003。

## 1. 真实连续起报样本与 OM 无损转换（FR-013、FR-014）

### 1.1 源数据固定清单（`validation/m2/fixtures/wb2_8run_source.json`）

- 数据集：`https://storage.googleapis.com/weatherbench2/datasets/ifs_ens/2018-2022-64x32_equiangular_conservative.zarr/`
- `.zmetadata` SHA-256：`47ed5b9565dc467ed253f41b9bb931d34de796d50fe760620937881c3a847145`
- 共同有效时刻：`2020-01-05T00:00:00Z`（Unix 秒 `1578182400`）
- 区域：纬度下标 `23..26`（`[42.1875, 47.8125, 53.4375]`），经度下标 `1..4`（`[5.625, 11.25, 16.875]`），提取后按 `(member=50, lat=3, lon=3)` C-order float32 保存。

| `ordinal` | `init_time` (UTC) | `lead_hours` | 源对象键（`2m_temperature/` 下） | `generation` | 源块字节 | 源块 SHA-256 | 50×3×3 `<f4` 载荷 SHA-256 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `0` | `2020-01-01T00:00:00Z` | `96` | `365.0.16.0.0` | `1704666510530808` | `1122639` | `83e26b640e60bcef8ea620e061867e2021e1da9f5570c14c1dabb1d173a597e5` | `3421338e84b53a1ee66407f89fabfd7f9b7f2c00ebe75110435f548e8544b319` |
| `1` | `2020-01-01T12:00:00Z` | `84` | `365.0.14.0.0` | `1704541276405280` | `1115286` | `962e5f3e2a450335f7d7fee9fbc7ab8decf75602151344e97b9299dd4532524a` | `3ec5984d226cd44e8e71e263f9a3ee7fef509746e3343fa8695d50a8dfb664f4` |
| `2` | `2020-01-02T00:00:00Z` | `72` | `365.0.12.0.0` | `1704559374438657` | `1106924` | `ab4640b5d4ae713d6e01c247ebb351c8f5ead86372818df921822dd69e1cb150` | `f5eb32986863f09106f84ce68e3cda92058d64a05c0131d4a4e6a4e0fa846757` |
| `3` | `2020-01-02T12:00:00Z` | `60` | `365.0.10.0.0` | `1704541243828388` | `1098382` | `4c3f87e970a487ab49c16745a563b32af23de7749221126f43bd6bc8569a2db4` | `92f2ba80f9517e746e5e733ef8015f432a540733155451ef4a3a514956d8d4eb` |
| `4` | `2020-01-03T00:00:00Z` | `48` | `366.0.8.0.0` | `1704541243344544` | `1076742` | `5396344277cf55e0f5f6f5d7b69eb7f14e89fa4edb362d69bf4bd62c0d27baea` | `c45d654bd5f6fcc2fc620bf8d2bb01afeb1cdce5f1e9e1fdde58096124f2f557` |
| `5` | `2020-01-03T12:00:00Z` | `36` | `366.0.6.0.0` | `1704545053780607` | `1061521` | `45aa298290173b61f113a161156e28bd710476d45d93fa9cba390372cad6a6d8` | `c21e56ef24b11ff30728998fa13376e019a36433280232ca81fe9ca02e5886fd` |
| `6` | `2020-01-04T00:00:00Z` | `24` | `366.0.4.0.0` | `1704668856985594` | `1040117` | `5d7c57976f93dad1470c0d2babc325b7abc713213464273dbf13456b0396a45b` | `6ac9759b4d55dc0690170841a6f07ab344f8780a8c046c1cc5cc290b916d464c` |
| `7` | `2020-01-04T12:00:00Z` | `12` | `366.0.2.0.0` | `1704591647405168` | `1019374` | `36399ddc10074a7204989648f1cafcfb782eba8b0c795eea2befa80a6f7c1492` | `34d34fe80aa3305f93f9b01db53029655bebe4d45a6aae70b24944ee8c4ba519` |

### 1.2 OM 文件写入规范

每批生成单个 `batch_<ordinal>_<init_utc>.om`：
- 根 group `""` 下唯一变量数组：`name="temperature_2m"`，`dtype=float32`，`shape=[50, 3, 3]`，`chunks=[50, 3, 3]`，`compression="fpx_xor_2d"`，`scale_factor=1.0`，`add_offset=0.0`；
- 挂在 `temperature_2m` 下的子属性（`children`）：
  - `coordinates`: scalar String `"member lat lon"`
  - `valid_time`: scalar Int64 `1578182400`
  - `latitude`: Float32[3] `fpx_xor_2d` `[42.1875, 47.8125, 53.4375]`
  - `longitude`: Float32[3] `fpx_xor_2d` `[5.625, 11.25, 16.875]`
  - `member_label`: Int64[50] `pfor_delta_2d` `[1, 2, ..., 50]`
  - `run_unix_seconds`: Int64[1] `pfor_delta_2d` `[<run_epoch>]`
  - `lead_seconds`: Int64[1] `pfor_delta_2d` `[<lead_hours * 3600>]`
  - `units`: scalar String `"K"`
  - `source_variable`: scalar String `"2m_temperature"`
- 写完立即用 `OmFileReader` 读回，断言 `compression_name == "fpx_xor_2d"` 且读回的 float32 原始字节与源载荷 `tobytes()` 100% 逐字节相同。

## 2. DuckOMO 发布包核对与加载隔离（FR-015）

### 2.1 固定发布包身份（Linux ARM64 / DuckDB v1.5.5）

| 字段 | 目标版本 `v0.3.5-rc.1` | 对照版本 `v0.3.4` |
| --- | --- | --- |
| ZIP 文件名 | `duckomo-v0.3.5-rc.1-duckdb-v1.5.5-linux_arm64.zip` | `duckomo-v0.3.4-duckdb-v1.5.5-linux_arm64.zip` |
| ZIP SHA-256 | `cee19b262b49fe501a69787149cec4f26cfbaa2899c137e17f0b6834dd099890` | `fd123e090a31ba69219eed4e613e56947ba503d127cc38c3d444aadfb929f713` |
| 根 `manifest.json` SHA-256 | `d7b36a255faa4ab02d21f01cb3384ea307ee4e20823bf456aa7a358dcabb8e6b` | `6a0c921b35f24fba599ad12623558a13cffebad1c72669c44da4f433f6315c54` |
| `SHA256SUMS` SHA-256 | `570f292c02178c02a4a6d514b3b222be5c708ba2450258f6603e46e20023504c` | `5f9a3c5f976836cb4abbd7331908ba779352d23ef9d07d49a9e3aa700075a1e9` |
| 解压后 `duckomo.duckdb_extension` SHA-256 | `586bc49de1da86c26b9efd46ff1adc07dca8e206967793dad88e20dd53f3047b` | `554e36f68fade713c19a94ac4db3788933c95c22db6ef288da047e2f3e620b80` |
| `source_commit` / `extension_version` | `55d66f3fbd3199d7471b8cd569c43cc91a8cec7e` / `55d66f3` | `c3084c6937d7d4de02a4edb7aaa68cdb5086f7f6` / `c3084c6` |

### 2.2 加载隔离与防冒充规则

每次启动 DuckDB CLI 执行 M2 读取或回放时，必须：
1. 将进程 `HOME` 指向本次运行专属的空目录；
2. 执行：
   ```sql
   SET TimeZone = 'UTC';
   SET extension_directory = '<本次运行专属空目录>';
   SET autoinstall_known_extensions = false;
   SET autoload_known_extensions = false;
   LOAD '<已核对哈希的 duckomo.duckdb_extension 绝对路径>';
   LOAD '<本次构建的 ensabc_m0_extension.duckdb_extension 绝对路径>';
   ```
3. 查询 `duckdb_extensions()`，断言 `duckomo` 的 `loaded = true`、`extension_version` 等于目标或对照版本的 7 位 `source_commit`，且 `install_mode = 'NOT_INSTALLED'`（证明未混入 `~/.duckdb/extensions/` 下的非发布本地构建）。

## 3. `read_om` 查询与准备阶段 SQL 门禁

```sql
CREATE MACRO m2_read_om_batch(om_path, ord, init_ts, lead_hr) AS TABLE
SELECT
  ord::INTEGER                       AS ordinal,
  init_ts::TIMESTAMP                 AS init_time,
  lead_hr::INTEGER                   AS lead_hours,
  valid_time                         AS valid_time,
  member::BIGINT                     AS member_label,
  om_source.grid_id                  AS grid_id,
  om_source.parent_point_index::INT  AS point_index,
  lat::DOUBLE                        AS latitude,
  lon::DOUBLE                        AS longitude,
  temperature_2m::DOUBLE             AS value_k
FROM read_om(
  om_path,
  variable       := 'temperature_2m',
  dimensions     := ['member', 'lat', 'lon'],
  grid           := {'nx': 3, 'ny': 3, 'lat0': 42.1875, 'lon0': 5.625,
                     'dlat': 5.625, 'dlon': 5.625, 'order': 'separate'},
  spatial_axes   := ['lat', 'lon'],
  axes           := {'member': {'axis': 'member', 'values': [
                       1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                       21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
                       41,42,43,44,45,46,47,48,49,50]}},
  include_source := true
);
```

**准备阶段必检 SQL 断言（在构造 `ens_cluster` 任务前执行）**：
- 每批 `count(*) == 450`、`count(value_k) == 450`（零 `NULL`）、`count(*) FILTER (WHERE NOT isfinite(value_k)) == 0`（零 `±Inf`）；
- 每批 `count(DISTINCT (member_label, point_index)) == 450`、`count(DISTINCT member_label) == 50`、`count(DISTINCT point_index) == 9`；
- 所有批次 `grid_id` 唯一且等于 `"ca3212142cd36c9704cd0aa1f47c24c905611eca7bb911427279997de133ad26"`；
- 每个 `point_index` 的 `(latitude, longitude)` 与清单 `points` 表精确一致；
- 所有行的 `valid_time == TIMESTAMP '2020-01-05 00:00:00'`，且 `init_time + to_hours(lead_hours) == valid_time`；
- 与清单成员表 `JOIN` 绑定 `(member_label -> member_index, member_id)` 后，按 `ORDER BY member_index` 聚合成员列表，每个成员内部按 `ORDER BY point_index` 聚合 9 个特征值。
