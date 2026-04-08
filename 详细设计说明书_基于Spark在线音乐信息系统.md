# 详细设计说明书（DDS）

## 1. 文档信息

| 项目 | 内容 |
|---|---|
| 文档名称 | 基于Spark的在线音乐信息系统详细设计说明书 |
| 文档编号 | DDS-MUSIC-SPARK-001 |
| 版本 | v1.0 |
| 日期 | 2026-04-08 |
| 依据文档 | 需求规格说明书（SRS-MUSIC-SPARK-001）、架构设计说明书（SAD-MUSIC-SPARK-001） |

## 2. 设计目标与边界

### 2.1 设计目标

1. 将 SRS 中 FR-01~FR-12 拆解为可开发、可测试的模块级设计。  
2. 明确核心链路的数据模型、接口契约、调度策略与异常处理。  
3. 在单机 Docker Compose 环境下保证演示级稳定性、可维护性和可追溯性。  
4. 满足性能目标：榜单延迟 <= 10 秒，查询平均响应 <= 2 秒，识曲响应 <= 5 秒（样本规模内）。

### 2.2 设计边界

1. 面向毕业设计环境，不涉及企业级多活容灾。  
2. 仅处理公开数据与演示样本，不涉及敏感隐私信息。  
3. 采用模块化单体业务服务 + 数据处理子系统，不拆分微服务网格。  
4. 重点覆盖 Must/Should 需求，Could 需求提供可落地基线实现。

## 3. 总体详细设计

### 3.1 分层与子系统

系统划分为六层：

1. 展示层（Vue 3 + ECharts）：看板、检索页、管理页。  
2. 接口层（Spring Boot）：统一 REST API、鉴权、参数校验、编排聚合。  
3. 计算层（Spark SQL + Structured Streaming）：离线统计与实时 TopN。  
4. 检索层（Python + Librosa + FAISS）：特征提取、索引构建、TopK 查询。  
5. 存储层（MySQL + 文件存储）：结构化结果、明细数据、向量与样本文件。  
6. 调度层（APScheduler + Cron）：采集、清洗、离线重算、索引更新。

### 3.2 运行容器与职责

1. nginx：统一入口与反向代理。  
2. frontend：前端静态资源服务。  
3. api-service：Spring Boot 业务 API。  
4. spark-job：批处理与流处理任务执行。  
5. kafka + zookeeper：行为事件缓冲与流输入。  
6. feature-service：识曲/哼唱与相似检索服务。  
7. mysql：维度表、事实表、系统管理表。  
8. storage：ODS/DWD/DWS/ADS 与音频文件挂载目录。

### 3.3 目录与代码组织建议

1. api-service  
   - controller：接口层  
   - service：业务编排  
   - repository/mapper：数据访问  
   - domain：实体与 DTO  
   - security：鉴权与权限  
   - task：后台任务入口  
2. spark-job  
   - jobs/offline：离线任务  
   - jobs/streaming：流式任务  
   - common：指标口径、公共 UDF、写入工具  
3. feature-service  
   - preprocess：音频预处理  
   - feature：特征提取  
   - index：索引构建与加载  
   - search：检索逻辑  

## 4. 需求到模块映射（RTD）

| 需求ID | 模块 | 主要实现点 | 关键输出 |
|---|---|---|---|
| FR-01 | 采集模块 | 平台适配器、调度触发、失败重试 | ODS 数据、任务日志 |
| FR-02 | 清洗模块 | 去重、标准化、缺失策略、异常标记 | DWD 数据、质量报告 |
| FR-03 | 存储模块 | 分层写入、历史归档、统一查询视图 | DWS/ADS、追溯信息 |
| FR-04 | 实时排行 | Kafka 消费、窗口聚合、TopN 快照 | fact_rank_snapshot |
| FR-05 | 音乐分类 | 规则/模型分类、多标签权重 | fact_song_classification |
| FR-06 | 听众分类 | 行为特征工程、KMeans 分群 | fact_user_segment |
| FR-07 | 群体画像 | 聚合标签、群体对比、趋势 | fact_user_portrait |
| FR-08 | 相似歌曲 | 特征向量化、FAISS 索引 | TopK 相似歌曲 |
| FR-09 | 听歌识曲 | 片段预处理、候选召回与重排 | 候选歌曲 + 置信度 |
| FR-10 | 哼唱搜歌 | 旋律特征抽取、旋律相似检索 | 候选歌曲 + 耗时 |
| FR-11 | 可视化 | 多视图图表、联动筛选、导出 | 看板与分析页 |
| FR-12 | 系统管理 | 登录鉴权、任务控制、参数配置 | 用户权限、审计日志 |

## 5. 详细模块设计

### 5.1 数据采集模块（FR-01）

#### 5.1.1 组件设计

1. SourceAdapter：平台采集适配器接口。  
2. CrawlExecutor：采集执行器，支持分页、限速与重试。  
3. SchemaMapper：字段映射与平台字段标准化。  
4. TaskLogger：任务生命周期日志写入。

#### 5.1.2 核心流程

1. APScheduler 触发采集任务。  
2. 读取平台配置并创建适配器。  
3. 执行抓取并写入 ODS 原始分区。  
4. 写入 sys_task_log（成功/失败、耗时、重试次数）。

#### 5.1.3 异常策略

1. 网络异常指数退避重试，默认 3 次。  
2. 字段缺失写入异常记录并进入待人工核验列表。  
3. 平台限流时自动降速并切换备用来源。

### 5.2 数据清洗治理模块（FR-02, FR-03）

#### 5.2.1 清洗规则

1. 去重：按（platform, platform_song_id）主键去重。  
2. 标准化：时间统一 UTC+8，数值统一为 bigint/double。  
3. 缺失值：关键字段缺失直接剔除，非关键字段按默认值填充。  
4. 实体映射：构建 unified_song_id，保证跨平台一致。  
5. 异常标记：score 异常、时长异常、乱码标签标记入异常表。

#### 5.2.2 输出

1. DWD 明细表（分区：dt/platform）。  
2. data_quality_report（每批次重复率、缺失率、异常率）。  
3. lineage_meta（来源批次、处理规则版本）。

### 5.3 实时排行模块（FR-04）

#### 5.3.1 输入与窗口

1. 输入主题：kafka.topic.music_play_event。  
2. 事件时间字段：event_time。  
3. Watermark：2 分钟。  
4. 窗口配置：1 分钟滚动窗口，支持日/周/月聚合任务复用。

#### 5.3.2 计算口径

热度分：

`hot_score = play_count*0.50 + like_count*0.25 + comment_count*0.15 + growth_rate*0.10`

其中 growth_rate 为当前窗口相对上一窗口增长率，防止异常值时进行上限截断。

#### 5.3.3 输出与幂等

1. 输出表：fact_rank_snapshot。  
2. 幂等键：rank_time + rank_type + item_id。  
3. 更新策略：先写临时表再 merge，避免重复写入。

### 5.4 分类分群与画像模块（FR-05, FR-06, FR-07）

#### 5.4.1 音乐分类

1. 输入：标签文本、歌词关键词、节奏特征。  
2. 方法：规则基线 + 轻量模型融合。  
3. 输出：主标签、副标签、标签权重、依据字段。

#### 5.4.2 听众分群

1. 特征：活跃天数、时段偏好、风格偏好占比、互动率。  
2. 算法：KMeans（默认 k=3，可配置）。  
3. 结果：cluster_id、cluster_name、特征中心。

#### 5.4.3 群体画像

1. 聚合维度：风格偏好、热门歌手、高活跃时段。  
2. 输出：群体标签 + 雷达图指标向量。  
3. 支持时间切片对比（本周 vs 上周）。

### 5.5 音频检索模块（FR-08, FR-09, FR-10）

#### 5.5.1 预处理

1. 音频统一采样率 22050Hz。  
2. 裁剪静音片段并进行幅度归一化。  
3. 单次输入最长 30 秒，超限自动截断。

#### 5.5.2 特征抽取

1. 通用特征：MFCC、Chroma、Tempo。  
2. 检索向量：拼接后 PCA 降维至固定长度。  
3. 可选扩展：深度 embedding 替换传统特征。

#### 5.5.3 索引与查询

1. 索引类型：FAISS IVF_FLAT（样本规模可切换为 Flat）。  
2. 检索流程：粗召回 TopN -> 相似度重排 -> 置信度归一化。  
3. 返回字段：song_id、song_name、artist_name、score、rank。

#### 5.5.4 成功判定

1. 识曲：Top1 分值 >= threshold_recognize 判定成功。  
2. 哼唱：TopK 中存在 score >= threshold_humming 判定有效候选。  
3. 无结果时返回可解释提示与重试建议。

### 5.6 API 与业务编排模块（FR-11, FR-12）

#### 5.6.1 统一响应结构

```json
{
  "code": 0,
  "message": "ok",
  "data": {},
  "trace_id": "uuid"
}
```

#### 5.6.2 核心接口清单

| 接口 | 方法 | 说明 | 权限 |
|---|---|---|---|
| /api/v1/rankings | GET | 查询实时/日周月榜 | USER |
| /api/v1/analytics/overview | GET | 概览指标 | USER |
| /api/v1/analytics/segments | GET | 听众分群结果 | USER |
| /api/v1/analytics/portraits | GET | 群体画像结果 | USER |
| /api/v1/retrieval/similar | POST | 相似歌曲检索 | USER |
| /api/v1/retrieval/recognize | POST | 听歌识曲 | USER |
| /api/v1/retrieval/humming | POST | 哼唱搜歌 | USER |
| /api/v1/tasks | GET | 任务列表与状态 | ADMIN |
| /api/v1/tasks/{id}/retry | POST | 失败任务重试 | ADMIN |
| /api/v1/configs | PUT | 参数配置更新 | ADMIN |

#### 5.6.3 参数校验要点

1. 时间范围不得超过可配置上限（默认 90 天）。  
2. TopK 取值范围 1~50。  
3. 上传音频类型限制为 wav/mp3，大小上限 20MB。  
4. 角色不足时返回 403 并记录审计日志。

## 6. 数据库详细设计

### 6.1 维度表

#### 6.1.1 dim_song

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| song_id | varchar(64) | unique | 统一歌曲ID |
| song_name | varchar(256) | not null | 歌曲名 |
| artist_id | varchar(64) | not null | 统一歌手ID |
| duration_sec | int |  | 时长秒 |
| tags | varchar(512) |  | 标签集合 |
| publish_time | datetime |  | 发布时间 |
| created_at | datetime | not null | 创建时间 |
| updated_at | datetime | not null | 更新时间 |

#### 6.1.2 dim_artist

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| artist_id | varchar(64) | unique | 统一歌手ID |
| artist_name | varchar(128) | not null | 歌手名 |
| region | varchar(64) |  | 地区 |
| style | varchar(128) |  | 风格 |
| created_at | datetime | not null | 创建时间 |
| updated_at | datetime | not null | 更新时间 |

### 6.2 事实表

#### 6.2.1 fact_play_event

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| user_id | varchar(64) | index | 用户ID |
| song_id | varchar(64) | index | 歌曲ID |
| play_time | datetime | index | 播放时间 |
| play_count | int |  | 播放次数 |
| like_count | int |  | 点赞次数 |
| comment_count | int |  | 评论次数 |
| source_platform | varchar(32) |  | 来源平台 |
| dt | date | partition | 分区日期 |

#### 6.2.2 fact_rank_snapshot

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| rank_time | datetime | index | 榜单时间 |
| rank_type | varchar(16) | index | minute/day/week/month |
| item_id | varchar(64) | index | 歌曲或歌手ID |
| item_type | varchar(16) |  | song/artist |
| score | double |  | 热度分 |
| rank_position | int |  | 排名位次 |
| version | varchar(32) |  | 计算版本 |

#### 6.2.3 fact_user_segment

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| stat_date | date | index | 统计日期 |
| user_id | varchar(64) | index | 用户ID |
| cluster_id | int | index | 群组ID |
| cluster_name | varchar(64) |  | 群组名称 |
| features_json | json |  | 特征快照 |

#### 6.2.4 fact_user_portrait

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| stat_date | date | index | 统计日期 |
| cluster_id | int | index | 群组ID |
| portrait_tags | json |  | 画像标签 |
| radar_metrics | json |  | 雷达指标 |

#### 6.2.5 fact_song_embedding

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| song_id | varchar(64) | unique | 歌曲ID |
| vector_path | varchar(256) |  | 向量文件路径 |
| vector_dim | int |  | 维度 |
| feature_version | varchar(32) |  | 特征版本 |
| updated_at | datetime |  | 更新时间 |

### 6.3 系统表

#### 6.3.1 sys_task_log

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| task_name | varchar(128) | index | 任务名称 |
| task_type | varchar(32) |  | collect/clean/offline/stream/index |
| status | varchar(16) | index | running/success/failed |
| retry_count | int |  | 重试次数 |
| start_time | datetime |  | 开始时间 |
| end_time | datetime |  | 结束时间 |
| duration_ms | bigint |  | 耗时 |
| message | varchar(1024) |  | 摘要信息 |
| trace_id | varchar(64) |  | 追踪ID |

#### 6.3.2 sys_config

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | bigint | PK | 主键 |
| config_key | varchar(128) | unique | 配置键 |
| config_value | varchar(1024) |  | 配置值 |
| value_type | varchar(32) |  | string/int/double/json |
| updated_by | varchar(64) |  | 更新人 |
| updated_at | datetime |  | 更新时间 |

## 7. 关键流程详细设计

### 7.1 采集到可视化链路

1. Scheduler 触发采集 -> 采集写 ODS。  
2. 清洗任务读取 ODS -> 写 DWD 与质量报告。  
3. 离线任务汇总 DWS/ADS -> MySQL 服务表。  
4. 前端通过 API 查询 ADS -> 渲染图表。

### 7.2 实时榜单链路

1. 行为事件写入 Kafka。  
2. Spark Streaming 消费并窗口聚合。  
3. 输出 fact_rank_snapshot。  
4. API 查询最新快照并返回前端。

### 7.3 识曲/哼唱链路

1. 前端上传音频到 API。  
2. API 转发 feature-service。  
3. feature-service 完成预处理、特征提取、FAISS 查询。  
4. 返回 TopK 候选，API 补充歌曲元信息后响应前端。

## 8. 配置与调度设计

### 8.1 核心配置项

| 配置键 | 默认值 | 说明 |
|---|---|---|
| ranking.window.seconds | 60 | 实时窗口长度 |
| ranking.watermark.seconds | 120 | 水位延迟 |
| ranking.topk | 50 | 榜单返回上限 |
| retrieval.topk.default | 10 | 检索默认TopK |
| retrieval.threshold.recognize | 0.78 | 识曲阈值 |
| retrieval.threshold.humming | 0.65 | 哼唱阈值 |
| collect.retry.max | 3 | 采集最大重试 |
| api.query.range.maxDays | 90 | 查询最大时间跨度 |

### 8.2 调度计划

1. 采集任务：每 30 分钟执行。  
2. 清洗任务：每小时执行。  
3. 离线汇总：每天 02:00 执行。  
4. 分群与画像：每天 03:00 执行。  
5. 特征索引重建：每天 04:00 执行。  
6. 实时任务：常驻运行，异常自动拉起。

## 9. 异常处理与可观测性设计

### 9.1 错误码设计

| 业务码 | HTTP码 | 场景 |
|---|---|---|
| 0 | 200 | 成功 |
| 1001 | 400 | 参数校验失败 |
| 1002 | 401 | 未登录 |
| 1003 | 403 | 无权限 |
| 2001 | 404 | 资源不存在 |
| 3001 | 500 | 数据处理失败 |
| 3002 | 504 | 检索超时 |

### 9.2 日志与追踪

1. API、任务、检索统一输出 trace_id。  
2. sys_task_log 记录任务生命周期。  
3. 关键链路记录输入规模、耗时、失败原因。  
4. 前端请求失败保留请求参数摘要用于复盘。

### 9.3 降级策略

1. 实时榜单不可用时回退到最近一次快照。  
2. 检索超时时返回缓存候选与提示信息。  
3. 外部采集失败时启用最近可用数据快照。

## 10. 安全设计

1. Spring Security + JWT 实现登录态与接口鉴权。  
2. 管理接口按 ADMIN 角色限制。  
3. 文件上传进行类型与大小校验，禁止可执行文件。  
4. SQL 访问通过 MyBatis 参数化，防止注入。  
5. 审计日志记录关键操作（配置变更、任务重试、权限失败）。  
6. 不采集敏感个人信息，仅保留业务演示所需匿名ID。

## 11. 性能与容量设计

### 11.1 性能目标

1. 实时榜单更新延迟 <= 10 秒。  
2. 查询接口 P95 <= 2 秒。  
3. 识曲/哼唱 P95 <= 5 秒。  
4. 演示环境并发 >= 50 查询请求稳定。

### 11.2 优化策略

1. ADS 层预聚合，减少在线计算。  
2. 热门榜单结果缓存 30~60 秒。  
3. 检索索引常驻内存并按版本热更新。  
4. 大查询限制时间跨度与分页大小。  
5. Spark 作业按主题拆分，降低单任务压力。

## 12. 测试设计要点

### 12.1 功能测试

1. 覆盖 FR-01~FR-12 的主流程与异常流程。  
2. 验证任务重试、参数更新、权限控制。  
3. 验证图表筛选联动和导出能力。

### 12.2 性能测试

1. 榜单持续刷新场景压测。  
2. 检索接口并发与大文件边界测试。  
3. API 慢查询追踪与 SQL 优化验证。

### 12.3 数据与算法测试

1. 清洗质量报告核对（唯一率、缺失率、异常率）。  
2. 分群结果稳定性评估（轮廓系数/群体可解释性）。  
3. 识曲与哼唱在样本集上的 Top1/TopK 命中率评估。

## 13. 交付与验收映射

| 交付物 | 对应章节 | 验收方式 |
|---|---|---|
| 模块代码与接口 | 第5、6章 | 联调与接口测试 |
| 调度配置与运行脚本 | 第8章 | 任务执行记录 |
| 监控与日志方案 | 第9章 | 日志抽样与故障演练 |
| 安全与权限实现 | 第10章 | 权限与审计测试 |
| 性能与测试报告 | 第11、12章 | 指标对比与报告审查 |

## 14. 设计结论

本详细设计在既有架构基础上完成了从需求到实现的可执行拆解：  
1. 覆盖采集、治理、计算、检索、服务、可视化与管理全链路。  
2. 明确了关键数据模型、接口契约、调度策略和容错机制。  
3. 对毕业设计环境下的性能、安全、可维护性给出了可验证方案。  
4. 可直接作为后续开发、测试与答辩材料的实施依据。
