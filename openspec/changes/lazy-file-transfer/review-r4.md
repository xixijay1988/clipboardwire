# 第 4 轮复审 — 两个终轮 Blocker 的修法验证

- 评审者：`7bc64b4e-7a11-4f9b-8c6e-79de0af7a05b`（Lody Reviewer）
- operationId：`cw-openspec-review-r4`
- 派发 04:22:52 · 子会话完成 04:25:26 · 投递 04:25:26
- 审查基线：HEAD `3da420e`，工作区快照（openspec 未纳入版本控制）

## 结论

| 项 | 状态 |
| --- | --- |
| Blocker A（pull 鉴权误伤） | **已清零** |
| Blocker B（digest 覆盖范围） | **未清零**（原问题修好，新规则过宽） |
| 阻塞 Phase 0 spike | 否 |
| 阻塞 Phase 5 实现 | 是 |

## Blocker A — 已清零

逐条核查后，没有任何身份规则会拒绝「接收端 X 向源端 S 发 `file_pull`」。
关键条款：`design.md:276-285`、`spec.md:538-542`（SHALL NOT 要求拉取方是
offer owner）、`tasks.md` 3.6。

`?device=` 剩余面已如实披露（凭据被窃即可接管设备），无虚假声明。

## Blocker B — 未清零

**已修好的部分**：中途起点的会话、只覆盖 `[k,N)` 的 digest 都被堵住；
缺口检测、abort、并发第二路、TTL 过期重 offer、同批次排队——五条路径均已
收敛到「从 index 0 重开」。

**新问题**：`design.md:439-440`、`tasks.md:138-139`、`spec.md:478-481`
写成「reject any `file_pull` whose `chunk_index != 0`」。但接收端是窗口化
拉取的（`design.md:460`，默认 3 块/窗），第二个窗口起点是 3，会被
`invalid_start` 拒掉 → 从 0 重开 → 同一处再拒 → 3 次后失败。
**任何超过一个窗口（默认 12 MiB）的文件都传不完**，与 `tasks.md` 5.9、
5.14 直接冲突。

附带的第二个漏洞：会话内 pull 未要求连续（`design.md:401` 只校验
`chunk_index < total_chunks`），跳到 9 也会被服务，顺序哈希漏掉中间块。

**建议改法**（一句话）：创建会话的 pull（`transfer_id` 未知）要求
`chunk_index = 0`；会话内后续 pull 要求 `chunk_index` 等于下一个尚未发送的
index；其余以 `invalid_start` 中止。

## 两处修改带出的其他新问题

- **Major（Phase 6 前必须定）**：paste 模式下「从 0 重开」没有定义。
  `spec.md:486-488` 的「overwriting bytes in place」只适用于 eager/pickup 的
  `.partial`；paste 模式下前 k 字节已交给 Explorer，无法覆盖。
- **Nit**：`tasks.md` 5.6 的「dedupe by index」已成死逻辑。

## Major 及以下标题清单

- `?device=` 缺失/格式非法未定义（影响 v0.3 与 Android 接入 v0.4 hub）
- 每文件最多重开 3 次 + 一律从 0 重来 → 链路不稳时大文件可能永远传不完
- `design.md:637` 与 `design.md:328-329`、`spec.md:322-326` 关于 hub 重启后
  TTL 是否重算矛盾
- 大量新 `device_id` 可占满全局 4096 offer 上限（在威胁模型内）

## 总评（原文）

> 文档不阻塞 spike。Phase 0 的 0.1、0.2 本来就不依赖协议定稿，现在就可以
> 开始；Phase 1（只加进度）也不受影响。文档仍阻塞 Phase 5 的实现，原因是
> B 的新规则写得过宽。按上面那一句话修正后，Phase 2 到 5 就可以动工。
> Phase 6 动手前，还需要先定下 paste 模式的重开规则，并等 spike 0.2 的结果。
