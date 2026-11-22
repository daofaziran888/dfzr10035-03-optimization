<template>
  <section class="page fault-page">
    <header class="page-head">
      <div>
        <h2>采集故障时间分布</h2>
        <p class="page-desc">
          按“同一端点 + 同一调用编号”还原采集失败到恢复的窗口；窗口与时段有重叠即纳入，
          开始和结束位置都会改变当期时长；没有恢复回执的窗口保持进行中，迟到日志只形成一次性更正。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="fault-refresh" type="button" @click="loadAll">刷新</button>
        <button class="btn primary" data-testid="fault-generate" type="button" @click="generateReport">
          生成治理报告
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="loadDistribution">
      <label class="filter-item">
        <span>时段开始</span>
        <input v-model="periodStart" data-testid="period-start" placeholder="2026-11-22 09:00" />
      </label>
      <label class="filter-item">
        <span>时段结束</span>
        <input v-model="periodEnd" data-testid="period-end" placeholder="2026-11-22 10:00" />
      </label>
      <label class="filter-item">
        <span>端点</span>
        <input v-model="endpoint" data-testid="endpoint-filter" placeholder="按端点过滤" />
      </label>
      <label class="filter-item">
        <span>分桶(分钟)</span>
        <input v-model.number="bucketMinutes" data-testid="bucket-minutes" type="number" min="1" max="1440" />
      </label>
      <button class="btn" data-testid="fault-query" type="submit">查询分布</button>
      <button class="btn ghost" type="button" @click="resetPeriod">重置时段</button>
    </form>

    <div class="stat-row">
      <article v-for="card in summaryCards" :key="card.label" class="stat-card" :data-testid="card.testid">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <p class="boundary-note" data-testid="boundary-rule">{{ distribution?.boundary_rule ?? '等待载入…' }}</p>

    <h3 class="section-title">分桶分布</h3>
    <div class="analysis-chart">
      <div
        v-for="bucket in distribution?.items ?? []"
        :key="bucket.时段"
        class="analysis-bar"
        :style="{ height: `${barHeight(bucket)}%` }"
        :title="`${bucket.时段} 故障${bucket.故障时长秒}秒 / 窗口${bucket.窗口数}个`"
        :data-testid="`bucket-${bucket.时段}`"
      >
        <span>{{ bucket.时段 }}</span>
      </div>
    </div>

    <h3 class="section-title">故障窗口（启停时间与影响规模）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>端点</th>
          <th>调用编号</th>
          <th>开始时间</th>
          <th>结束时间</th>
          <th>窗口状态</th>
          <th>持续秒数</th>
          <th>当期重叠秒数</th>
          <th>失败日志</th>
          <th>恢复日志</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in distribution?.windows ?? []"
          :key="row.窗口键"
          :data-testid="`window-${row.窗口键}`"
          :data-state="row.状态"
        >
          <td>{{ row.端点 }}</td>
          <td>{{ row.调用编号 }}</td>
          <td>{{ row.开始时间 }}</td>
          <td>{{ row.结束时间 ?? '—（无恢复回执）' }}</td>
          <td>
            <span :class="['state-chip', row.状态 === '已结束' ? 'state-done' : 'state-ongoing']">
              {{ row.状态 }}
            </span>
          </td>
          <td>{{ row.持续秒数 ?? '—' }}</td>
          <td>{{ row.当期重叠秒数 ?? '—' }}</td>
          <td>{{ row.失败日志编号.join(', ') || '—' }}</td>
          <td>{{ row.恢复日志编号.join(', ') || '—' }}</td>
        </tr>
        <tr v-if="!(distribution?.windows ?? []).length">
          <td colspan="9" class="empty-state">该时段没有可还原的故障窗口</td>
        </tr>
      </tbody>
    </table>

    <p v-if="(distribution?.unmatched_recoveries ?? []).length" class="warn-note" data-testid="unmatched-recoveries">
      有 {{ distribution?.unmatched_recoveries.length }} 条恢复回执缺少真实调用编号，未计入任何窗口：
      {{ (distribution?.unmatched_recoveries ?? []).map((item) => item.日志编号).join('、') }}
    </p>

    <h3 class="section-title">录入采集日志</h3>
    <form class="analysis-form" @submit.prevent="ingestLog">
      <label>
        <span>端点</span>
        <input v-model="logForm.端点" data-testid="log-endpoint" placeholder="ENDP-CSV-0001" />
      </label>
      <label>
        <span>调用编号（真实调用）</span>
        <input v-model="logForm.调用编号" data-testid="log-call-no" placeholder="CALL-0905-A" />
      </label>
      <label>
        <span>事件类型</span>
        <select v-model="logForm.事件类型" data-testid="log-kind">
          <option value="失败">失败</option>
          <option value="恢复">恢复</option>
        </select>
      </label>
      <label>
        <span>发生时间</span>
        <input v-model="logForm.发生时间" data-testid="log-time" placeholder="2026-11-22 09:05" />
      </label>
      <label>
        <span>日志编号（可选）</span>
        <input v-model="logForm.日志编号" data-testid="log-no" placeholder="FLOG-A1" />
      </label>
      <button class="btn primary" data-testid="log-submit" type="submit">纳入日志</button>
    </form>
    <p v-if="ingestMessage" class="result-note" data-testid="ingest-result">{{ ingestMessage }}</p>
    <ul v-if="ingestDestinations.length" class="destination-list" data-testid="ingest-destinations">
      <li v-for="(item, index) in ingestDestinations" :key="index">
        去向：{{ item.报告编号 ?? '未进入报告' }} · {{ item.结果 ?? `更正${item.更正编号}（${item.更正状态}）` }}
      </li>
    </ul>

    <h3 class="section-title">迟到更正与去向</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>更正编号</th>
          <th>调用编号</th>
          <th>更正类型</th>
          <th>原值 → 新值（开始/结束/持续）</th>
          <th>报告编号</th>
          <th>更正状态</th>
          <th>归档版本</th>
          <th>去向说明</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in corrections" :key="row.更正编号" :data-testid="`correction-${row.更正编号}`">
          <td>{{ row.更正编号 }}</td>
          <td>{{ row.调用编号 }}</td>
          <td>{{ row.更正类型 }}</td>
          <td>
            {{ formatWindow(row.原值) }} → {{ formatWindow(row.新值) }}
          </td>
          <td>{{ row.报告编号 ?? '—' }}</td>
          <td>{{ row.状态 }}</td>
          <td>{{ (row.去向?.归档版本 ?? []).join(', ') || '—' }}</td>
          <td>{{ row.去向?.结果 }}</td>
        </tr>
        <tr v-if="!corrections.length">
          <td colspan="8" class="empty-state">暂无迟到更正</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">治理报告取用与对账</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>报告编号</th>
          <th>评估范围</th>
          <th>状态</th>
          <th>窗口数</th>
          <th>影响调用数</th>
          <th>得分</th>
          <th>对账</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="report in reports" :key="report.报告编号" :data-testid="`report-${report.报告编号}`">
          <td>{{ report.报告编号 }}</td>
          <td>{{ report.评估范围 }}</td>
          <td>{{ report.status }}</td>
          <td>{{ report.fault_scope?.summary?.窗口数 }}</td>
          <td>{{ report.fault_scope?.summary?.影响调用数 }}</td>
          <td>{{ report.当前得分 }}</td>
          <td>
            <span v-if="report.对账" :class="report.对账.采用范围一致 ? 'state-chip state-done' : 'state-chip state-bad'">
              {{ report.对账.采用范围一致 && report.对账.跨报告无重复 ? '一致' : '不一致' }}
            </span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" @click="recalculate(report)">按原依据复算</button>
            <button class="link" type="button" @click="archiveReport(report)">归档</button>
            <button class="link" type="button" @click="checkConsistency(report)">核对</button>
            <button class="link" type="button" @click="traceCall(report)">追踪调用</button>
          </td>
        </tr>
        <tr v-if="!reports.length">
          <td colspan="8" class="empty-state">暂无采用采集故障分布的治理报告</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">调用计数核对</h3>
    <form class="analysis-form" @submit.prevent="traceByInput">
      <label>
        <span>调用编号</span>
        <input v-model="traceCallNo" data-testid="trace-call-no" placeholder="CALL-0905-A" />
      </label>
      <button class="btn" data-testid="trace-submit" type="submit">核对计数</button>
    </form>
    <div v-if="trace" class="trace-panel" data-testid="trace-result">
      <p>
        调用 {{ trace.调用编号 }}：日志 {{ trace.logs_total }} 条（失败 {{ trace.failure_logs }}、恢复
        {{ trace.recovery_logs }}），分布窗口 {{ trace.window_total }} 个。
      </p>
      <p>
        报告计入次数 {{ trace.counting.报告计入次数.join('/') || '0' }}，归档计入次数
        {{ trace.counting.归档计入次数.join('/') || '0' }}，恰好一次：
        <strong>{{ trace.counting.恰好一次 ? '是' : '否' }}</strong>
      </p>
      <ul>
        <li v-for="row in trace.reports" :key="row.报告编号">
          {{ row.报告编号 }}（{{ row.报告状态 }}）：采用 {{ row.采用的开始时间 ?? '—' }} ~
          {{ row.采用的结束时间 ?? '进行中' }}，已应用更正
          {{ (row.已应用更正编号 ?? []).join(', ') || '无' }}，归档版本 {{ row.归档版本 ?? '未归档' }}
        </li>
      </ul>
    </div>

    <footer class="page-foot">
      <span>{{ statusLine }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="fault-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson, request } from '@/api/client'

type WindowRow = {
  窗口键: string
  端点: string
  调用编号: string
  开始时间: string
  结束时间: string | null
  状态: string
  持续秒数: number | null
  当期重叠秒数?: number
  失败日志编号: number[]
  恢复日志编号: number[]
}

type Distribution = {
  period_start: string
  period_end: string
  boundary_rule: string
  summary: {
    窗口数: number
    已结束窗口数: number
    进行中窗口数: number
    影响调用数: number
    已结束故障时长秒: number
    影响规模: { 调用数: number; 窗口数: number; 失败日志数: number; 最长故障时长秒: number }
  }
  items: { 时段: string; 故障时长秒: number; 进行中观察秒数: number; 窗口数: number; 影响调用数: number }[]
  windows: WindowRow[]
  unmatched_recoveries: { 日志编号: string }[]
}

type Correction = {
  更正编号: string
  调用编号: string
  更正类型: string
  状态: string
  报告编号: string | null
  原值: Record<string, string | number | null>
  新值: Record<string, string | number | null>
  去向: { 归档版本?: string[]; 结果?: string }
}

type ReportRow = {
  报告编号: string
  评估范围: string
  status: string
  当前得分: number
  fault_scope?: { summary?: { 窗口数: number; 影响调用数: number } }
  对账?: { 采用范围一致: boolean; 跨报告无重复: boolean }
}

const periodStart = ref('2026-11-22 09:00')
const periodEnd = ref('2026-11-22 10:00')
const endpoint = ref('')
const bucketMinutes = ref(15)
const distribution = ref<Distribution | null>(null)
const corrections = ref<Correction[]>([])
const reports = ref<ReportRow[]>([])
const trace = ref<Record<string, any> | null>(null)
const traceCallNo = ref('CALL-0905-A')
const ingestMessage = ref('')
const ingestDestinations = ref<{ 报告编号: string | null; 结果?: string; 更正编号?: string; 更正状态?: string }[]>([])
const errorMessage = ref('')
const logForm = ref({ 端点: 'ENDP-CSV-0001', 调用编号: '', 事件类型: '失败', 发生时间: '', 日志编号: '' })

const summaryCards = computed(() => {
  const summary = distribution.value?.summary
  return [
    { label: '故障窗口', value: summary?.窗口数 ?? 0, testid: 'summary-windows' },
    { label: '影响调用（按调用编号）', value: summary?.影响调用数 ?? 0, testid: 'summary-calls' },
    { label: '已结束故障时长（秒）', value: summary?.已结束故障时长秒 ?? 0, testid: 'summary-seconds' },
    { label: '进行中窗口（缺恢复回执）', value: summary?.进行中窗口数 ?? 0, testid: 'summary-ongoing' },
  ]
})

const statusLine = computed(
  () => `时段 ${distribution.value?.period_start ?? '—'} ~ ${distribution.value?.period_end ?? '—'}，更正 ${corrections.value.length} 条，报告 ${reports.value.length} 份`,
)

function barHeight(bucket: { 故障时长秒: number }): number {
  const max = Math.max(1, ...(distribution.value?.items ?? []).map((item) => item.故障时长秒))
  return Math.max(4, Math.round((bucket.故障时长秒 / max) * 100))
}

function formatWindow(value: Record<string, string | number | null> | undefined): string {
  if (!value) return '—'
  return `${value.开始时间 ?? '—'} / ${value.结束时间 ?? '进行中'} / ${value.持续秒数 ?? '—'}s`
}

function queryParams(): string {
  const params = new URLSearchParams()
  if (periodStart.value) params.set('period_start', periodStart.value)
  if (periodEnd.value) params.set('period_end', periodEnd.value)
  if (endpoint.value) params.set('endpoint', endpoint.value)
  params.set('bucket_minutes', String(bucketMinutes.value || 15))
  return params.toString()
}

async function loadDistribution() {
  errorMessage.value = ''
  try {
    distribution.value = await fetchJson<Distribution>(`/api/fault-distribution?${queryParams()}`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分布读取失败'
  }
}

async function loadCorrections() {
  try {
    const payload = await fetchJson<{ items: Correction[] }>('/api/fault-distribution/corrections')
    corrections.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '更正读取失败'
  }
}

async function loadReports() {
  try {
    const payload = await fetchJson<{ items: ReportRow[] }>('/api/fault-distribution/reports')
    reports.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '报告读取失败'
  }
}

async function loadAll() {
  await Promise.all([loadDistribution(), loadCorrections(), loadReports()])
}

function resetPeriod() {
  periodStart.value = ''
  periodEnd.value = ''
  void loadDistribution()
}

async function postJson(path: string, body: unknown): Promise<any> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = await response.json()
  if (!response.ok) throw new Error(payload.detail?.message ?? payload.detail ?? '操作失败')
  return payload
}

async function ingestLog() {
  ingestMessage.value = ''
  ingestDestinations.value = []
  errorMessage.value = ''
  try {
    const payload = await postJson('/api/fault-distribution/logs', { values: { ...logForm.value } })
    ingestMessage.value = `${payload.message}${payload.late ? '（迟到日志）' : ''}`
    ingestDestinations.value = payload.destinations ?? []
    if (!payload.late) logForm.value.调用编号 = ''
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '日志录入失败'
  }
}

async function generateReport() {
  errorMessage.value = ''
  try {
    const payload = await postJson('/api/fault-distribution/reports', {
      values: { 时段开始: periodStart.value, 时段结束: periodEnd.value },
    })
    ingestMessage.value = payload.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '报告生成失败'
  }
}

async function recalculate(report: ReportRow) {
  errorMessage.value = ''
  try {
    const payload = await postJson(`/api/fault-distribution/reports/${report.报告编号}/recalculate`, {})
    ingestMessage.value = payload.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复算失败'
  }
}

async function archiveReport(report: ReportRow) {
  errorMessage.value = ''
  try {
    const payload = await postJson(`/api/fault-distribution/reports/${report.报告编号}/archive`, {})
    ingestMessage.value = payload.message
    await loadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档失败'
  }
}

async function checkConsistency(report: ReportRow) {
  errorMessage.value = ''
  try {
    const payload = await fetchJson<any>(`/api/fault-distribution/reports/${report.报告编号}/consistency`)
    ingestMessage.value = payload.consistent
      ? `${report.报告编号} 报告汇总与归档统计采用范围一致`
      : `${report.报告编号} 对账不一致，请检查采用范围`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '对账失败'
  }
}

async function traceCall(report: ReportRow) {
  const reportDetail = await fetchJson<any>(`/api/fault-distribution/reports/${report.报告编号}`)
  const callNo = reportDetail?.fault_scope?.adopted_call_numbers?.[0]
  if (callNo) {
    traceCallNo.value = callNo
    await traceByInput()
  }
}

async function traceByInput() {
  errorMessage.value = ''
  try {
    trace.value = await fetchJson<any>(`/api/fault-distribution/trace/${traceCallNo.value}`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '追踪失败'
  }
}

onMounted(loadAll)
</script>

<style scoped>
.boundary-note { color: var(--muted); font-size: 12px; margin: 0 0 8px; }
.section-title { font-size: 14px; margin: 18px 0 8px; }
.state-chip { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; }
.state-done { background: #e7f7ec; color: #15803d; }
.state-ongoing { background: #fff4e5; color: #b45309; }
.state-bad { background: #fdecec; color: #b42318; }
.warn-note { color: #b45309; font-size: 12px; }
.result-note { color: #15803d; font-size: 12px; }
.destination-list { font-size: 12px; color: var(--muted); margin: 4px 0 0; }
.trace-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-size: 13px; }
</style>
