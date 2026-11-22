# 网关工坊 · API接口采集与数据网关平台

面向第三方API接入注册、接口鉴权编排、限流熔断、响应解析、schema映射、结果缓存、重放恢复与交付订阅的API接口采集与数据网关平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 接口提供方 | `api_provider` | API提供方 | 提供方编号、提供方名称、认证方式 |
| 鉴权配置 | `auth_profile` | 鉴权方案 | 方案编号、提供方、鉴权类型 |
| 接口定义 | `endpoint_def` | 接口端点 | 端点编号、提供方、请求路径 |
| 限流熔断 | `rate_limit` | 限流策略 | 策略编号、目标端点、QPS上限 |
| 请求批次 | `request_batch` | 调用批次 | 批次编号、关联端点、参数模板 |
| 响应解析 | `response_parse` | 解析规则 | 规则编号、目标端点、JSON路径 |
| Schema差异 | `schema_diff` | 差异记录 | 记录编号、关联端点、变更字段 |
| 结果缓存 | `data_cache` | 缓存策略 | 策略编号、目标端点、缓存TTL |
| 重放队列 | `retry_queue` | 重放任务 | 任务编号、失败端点、失败原因 |
| 数据投递 | `data_delivery` | 投递管道 | 管道编号、数据来源、目标地址 |
| 订阅管理 | `subscriber` | 订阅方 | 订阅编号、订阅方、数据主题 |
| 接口监控 | `api_monitor` | 监控指标 | 指标编号、目标端点、监控类型 |
| Webhook路由 | `webhook_router` | 回调规则 | 规则编号、触发事件、目标URL |
| 传输加密 | `data_encrypt` | 加密配置 | 配置编号、数据范围、加密算法 |
| 调用日志 | `call_log` | 调用记录 | 记录编号、接口端点、请求参数 |
| 数据版本 | `data_version` | 数据快照 | 版本编号、数据来源、行数统计 |
| 管道审计 | `pipeline_audit` | 审计记录 | 记录编号、管道名称、操作人 |
| 治理报告 | `govern_report` | 治理报告 | 报告编号、评估范围、合规指标 |
| 采集故障分布 | `fault_distribution` | 故障窗口/更正 | 端点、调用编号、开始/结束时间、状态 |
| 增量追踪 | `incr_tracker` | 增量标记 | 标记编号、目标端点、增量字段 |
| 抽样校验 | `sample_check` | 抽样计划 | 计划编号、目标端点、抽样比例 |
| 用量计费 | `cost_account` | 用量账单 | 账单编号、提供方、计费周期 |
| 任务调度 | `task_schedule` | 调度计划 | 计划编号、关联端点、Cron表达式 |

## 采集故障时间分布

跨模块链路，后端实现在 `backend/app/services/fault_distribution.py`，前端看板在
`frontend/src/views/FaultDistributionView.vue`（导航「采集故障分布」，路由
`/fault-distribution`）。它把“采集失败 / 采集恢复”日志还原成故障窗口，供治理报告取用。

口径约定：

- **同一真实调用关联**：失败与恢复按 `(端点, 调用编号)` 归并，同端点的其他调用
  不会提前结束本窗口；缺少调用编号的恢复回执只进未匹配清单，不计入任何窗口。
- **窗口归属**：窗口与时段有重叠即纳入；持续时长按与时段的重叠部分计算，
  窗口开始或结束位置变化都会重算当期数据。
- **未结束不猜结束**：窗口开始取该调用最早的失败日志，结束取其后最早的恢复回执；
  没有恢复回执的窗口状态为「进行中」，其时长只作为“已观察时长”展示。
- **报告原依据复算**：生成报告时固化采用窗口与依据日志编号；迟到日志形成“更正”，
  复算时按更正编号 + 日志编号去重，同一笔调用只计一次。
- **归档幂等与对账**：归档中断后重试会先核对等待中的更正是否已进入报告，
  复用同一份数据版本快照；报告汇总与归档统计的窗口数、调用数、采用范围必须一致。
- **更正去向**：看板列出每条更正所去报告、更正状态与归档版本，未进报告时说明只进入分布视图。

主要接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/fault-distribution` | 分桶分布 + 窗口清单 + 汇总（`period_start`/`period_end`/`endpoint`/`bucket_minutes`） |
| GET | `/api/fault-distribution/windows` | 只看窗口 |
| POST | `/api/fault-distribution/logs` | 录入失败/恢复日志，返回迟到更正及去向 |
| GET | `/api/fault-distribution/corrections` | 更正清单与去向 |
| POST | `/api/fault-distribution/reports` | 按时段生成治理报告（`GOVE-FAULT-*`） |
| POST | `/api/fault-distribution/reports/{ref}/recalculate` | 按原依据复算 |
| POST | `/api/fault-distribution/reports/{ref}/archive` | 归档并核对（幂等） |
| GET | `/api/fault-distribution/reports/{ref}/consistency` | 报告汇总与归档统计对账 |
| GET | `/api/fault-distribution/trace/{call_no}` | 核对一笔调用在分布/报告/归档中的计数 |

故障分布报告同样出现在「治理报告」模块，归档动作复用同一幂等流程；
`GET /api/chain/govern-score` 额外输出故障报告数与待应用更正数。

测试：`make test`（`backend/tests/test_fault_distribution.py`，标准库 unittest）。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
