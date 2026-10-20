# Rust DuckDB 扩展加载与嵌套数据往返 Spike

日期：2026-10-20

## 1. 范围与环境

本 spike 验证 Linux aarch64 下独立 Rust DuckDB 扩展的构建、元数据封装、实际宿主加载及 `ens_m0_echo` 嵌套数据往返。这是规划期实验，不实现聚类，也不等同于正式 M0 的 FR-007 至 FR-012 完整验收。

- 平台：Linux aarch64（Ubuntu 24.04.4 LTS）
- Rust：`rustc 1.96.0` / `cargo 1.96.0`，edition 2024
- 宿主：`duckdb v1.5.5 (d8cdaa33fd)`
- 锁定依赖：`duckdb = "=1.10505.0"`（`default-features = false`，启用 `loadable-extension`、`vscalar`、`vscalar-arrow`），`libduckdb-sys = 1.10505.0`，见 `Cargo.lock`
- 元数据封装脚本：`append_extension_metadata.py`（默认读取本地 `/home/blizhan/repo/github/duckomo/extension-ci-tools`，固定修订 `b777c70d30942cca5bef62d6d4fa23a13362f398`，可通过环境变量 `EXTENSION_CI_TOOLS` 覆盖路径并保留修订校验）

## 2. 签名与实现路径

```sql
ens_m0_echo(
    members STRUCT(member_id VARCHAR, features DOUBLE[])[]
) -> STRUCT(member_id VARCHAR, features DOUBLE[])[]
```

- `task_id` 保留在调用方 SQL 行中，不进入 UDF 参数。
- 当前 spike 通过 `VArrowScalar` 与 `#[duckdb_entrypoint_c_api(ext_name = "ensabc_m0_extension", min_duckdb_version = "v1.5.5")]` 注册标量函数，并封装为 `C_STRUCT_UNSTABLE`、`linux_arm64`、`v1.5.5` 扩展二进制。
- 编译与中间产物输出到 `/tmp/ensabc-m0-extension-target`，不写入仓库目录。

## 3. 实测结论

复跑命令：

```bash
bash specs/001-m0-baseline-validation/spikes/extension/verify.sh
```

已通过的实测项：

- 新建 DuckDB CLI 进程加载 `/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension` 成功。
- 多个合法任务行往返保持 `task_id` 与嵌套 `STRUCT(member_id VARCHAR, features DOUBLE[])[]` 顺序和数值一致。
- 2100 个逐行变化任务跨内部向量分块（2048 行边界）完整消费并通过核验。
- 单行 3000 个成员（每成员 2 个特征）的大列表往返通过。
- 嵌套层 `NULL`（成员项 `NULL`、`member_id` `NULL`、`features` `NULL`、特征元素 `NULL`）及非有限值在 `invoke` 内触发 `Invalid Input Error`。
- 在同一 CLI 会话（`.bail off`）中，未包裹 `TRY` 的嵌套 `NULL` 查询触发真实错误后，后续合法查询仍在同一会话中成功执行。

## 4. 已发现的契约差距与阻塞项

1. **顶层 `NULL` 未报错（FR-009 阻塞项）**：调用 `ens_m0_echo(NULL::STRUCT(member_id VARCHAR, features DOUBLE[])[])` 时，DuckDB 默认 NULL 传播直接返回 `NULL` 而不调用 `invoke`。检查锁定 crate `duckdb-1.10505.0` 的 `src/vscalar/function.rs` 与 `src/vscalar/mod.rs` 可知，高层 `ScalarFunction` 未暴露 `duckdb_scalar_function_set_special_handling`（该符号存在于 `libduckdb-sys-1.10505.0`）。
2. **任务内特征不等长与空/重复 `member_id` 尚未在 spike 中拦截**：当前 `src/lib.rs` 仅校验了空列表、各嵌套层 `NULL` 和非有限浮点数，尚未校验空字符串 `member_id`、任务内重复 `member_id` 及任务内特征长度一致性。
3. **未覆盖的正式 M0 门禁**：双连接并发隔离、1/10/100 任务耗时与峰值 RSS 测量、无参考工具的最小运行环境验证尚未在本 spike 中执行。

## 5. 正式 M0 实现路径（含 `libduckdb-sys-1.10505.0` 符号核查）

- 已核查 `libduckdb-sys-1.10505.0/src/bindgen_bundled_version_loadable.rs`：loadable-extension 函数表已初始化 `duckdb_connect`、`duckdb_create_scalar_function`、`duckdb_scalar_function_set_special_handling`、`duckdb_scalar_function_set_error`、`duckdb_register_scalar_function`、`duckdb_list_vector_get_child`、`duckdb_struct_vector_get_child`、`duckdb_validity_row_is_valid`、`duckdb_string_t_length`、`duckdb_string_t_data` 与 `duckdb_vector_reference_vector`。
- 在正式扩展 crate 中直接使用 `duckdb::ffi` 的 C API 注册标量函数并开启 `duckdb_scalar_function_set_special_handling`，使顶层 `NULL` 进入回调并明确报错；校验通过后调用 `duckdb_vector_reference_vector(output, input_vec)` 原样返回，无需 fork `duckdb` crate。
- 在回调边界使用 `std::panic::catch_unwind` 防止 panic 跨 FFI，并补齐空 `member_id`、重复 `member_id`、任务内特征等长校验。
- 为全部 10 类非法输入逐一执行同连接“合法 -> 非法报错 -> 合法”恢复验证，以及双连接并发、1/10/100 任务测量和最小环境隔离验证。
