import { createRouter, createWebHistory } from 'vue-router'

import FaultDistributionView from '@/views/FaultDistributionView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/fault-distribution', name: 'fault-distribution', component: FaultDistributionView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
