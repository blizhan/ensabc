# M0 SQL 接口契约

本契约只定义诊断往返，不是 M1 聚类接口。实现入口为独立 DuckDB 扩展，输入输出不访问文件、网络或数据库历史状态。

## 签名与任务边界

```sql
ens_m0_echo(
    members STRUCT(member_id VARCHAR, features DOUBLE[])[]
) -> STRUCT(member_id VARCHAR, features DOUBLE[])[]
```

一行输入代表一个任务。`task_id` 由调用者在同一 SELECT 保留，不是函数参数，也不能从成员 ID 推导。成员 ID 与特征绑定在同一 STRUCT；构造列表时显式指定成员顺序。函数保留列表内的成员顺序和特征顺序，不承诺查询行的隐式顺序。

实现后使用示例：

```sql
SELECT task_id, ens_m0_echo(members) AS echoed
FROM (
    VALUES (
        'example',
        [
            {'member_id': 'p01', 'features': [281.0, 282.0]::DOUBLE[]},
            {'member_id': 'p02', 'features': [279.0, 280.0]::DOUBLE[]}
        ]::STRUCT(member_id VARCHAR, features DOUBLE[])[]
    )
) AS tasks(task_id, members)
ORDER BY task_id;
```

返回完整相同的结构和值；不增加簇编号、概率、排序或聚合。非空字符串 ID 保留原文，不 trim、不大小写折叠。数值相同的不同成员合法。

## 校验与失败

按照 [验证契约](validation.md) 拒绝各层 NULL、空列表、空 ID、重复 ID、不等长特征和非有限值。所有错误包含函数名、原因以及能够识别的成员索引/字段/特征索引；顶层错误标明任务输入层。

顶层 NULL 也必须进入明确拒绝路径，不接受 DuckDB 默认的 NULL-in/NULL-out 作为通过。因此实现须注册 special NULL handling，并为嵌套向量逐层读取有效性。类型错误可由 binder 拒绝，类型正确但数据非法由函数报告。

函数不得 panic 穿过 FFI 边界，不应因错误终止宿主进程。异常查询失败后同一连接继续接受合法查询。批次中出现非法任务时整条查询不作为成功结果交付；验证程序必须完整取回结果才判断成功。

## 多任务和版本约束

- 每个任务单独校验形状；不同任务可以有不同成员数、特征数。
- 向量处理按实际行数、列表 offset/length 和有效位图读取，不能将整个 child vector 当成某一行。
- 测试必须分别跨越成员列表长度和任务行批量边界；使用实际宿主批量大小记录证据。
- 两个独立连接并发执行的结果按调用者任务身份核对；不共享可变结果缓冲区。
- 扩展只承诺锁定平台和宿主版本。版本不匹配应拒绝加载，不通过关闭兼容性检查来适配。
- 本地未签名扩展可在显式启用 unsigned extensions 的测试宿主加载；该开关只用于签名验证，不能绕过版本检查。

是否已满足本契约以 `spikes/extension/findings.md` 和后续正式验收记录为准，不能由签名设计本身推断。
