use super::LoadBalancingPolicy;
use crate::core::Worker;
use agent_router::{RequestOutcome, RoutingContext};
use std::sync::Arc;

pub struct RequestLifecycle {
    policy: Arc<dyn LoadBalancingPolicy>,
    worker: Arc<dyn Worker>,
    context: Arc<RoutingContext>,
    finished: bool,
}

impl std::fmt::Debug for RequestLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestLifecycle")
            .field("policy", &self.policy.name())
            .field("worker", &self.worker.url())
            .field("finished", &self.finished)
            .finish()
    }
}

impl RequestLifecycle {
    pub fn start(
        policy: Arc<dyn LoadBalancingPolicy>,
        worker: Arc<dyn Worker>,
        context: Arc<RoutingContext>,
    ) -> Self {
        policy.on_request_start(&worker, &context);
        Self {
            policy,
            worker,
            context,
            finished: false,
        }
    }

    pub fn finish(&mut self, outcome: RequestOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.policy
            .on_request_finish(&self.worker, &self.context, outcome);
    }
}

impl Drop for RequestLifecycle {
    fn drop(&mut self) {
        self.finish(RequestOutcome::Cancelled);
    }
}
