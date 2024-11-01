import { SchedulerDurableObject } from './scheduler';

export { SchedulerDurableObject };

// Handle incoming requests
export default {
  async fetch(request, env) {
    // Use a unique ID for the Durable Object (e.g., a constant ID)
    const id = env.MSGBIND_SCHEDULER_DO.idFromName('SCHEDULER_INSTANCE');
    const obj = env.MSGBIND_SCHEDULER_DO.get(id);
    return obj.fetch(request);
  },
};
