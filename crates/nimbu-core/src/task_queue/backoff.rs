use std::time::Duration;

use crate::{BackoffStrategy, Task};

pub fn compute_backoff(task: &Task) -> Option<Duration> {
    let policy = task.retry_policy.as_ref()?;

    match &policy.strategy {
        BackoffStrategy::Fixed(delay) => Some(*delay),

        BackoffStrategy::Exponential {
            base,
            factor,
            max_delay,
        } => {
            let exp = factor.saturating_pow(task.attempts);
            let delay = base.saturating_mul(exp);
            Some(delay.min(*max_delay))
        }
    }
}
