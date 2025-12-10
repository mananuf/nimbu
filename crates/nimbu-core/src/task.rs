use std::{fmt::Display, time::SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{JobId, TaskId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed { attempt: u32, error: String },
    FailedPermanent { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RetryPolicyBuilder {
    pub max_retries: Option<u32>,
    pub backoff_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum TaskTransitionError {
    #[error("illegal task transition from {from} to {to}")]
    Illegal {
        from: &'static str,
        to: &'static str,
    },

    #[error("maximum retries limit exceeded")]
    RetryLimitedExceeded,
}

impl Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Assigned => write!(f, "Assigned"),
            TaskStatus::Pending => write!(f, "Pending"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Completed => write!(f, "Completed"),
            TaskStatus::Failed { attempt, error } => {
                write!(f, "Failed(attempt={}, error={})", attempt, error)
            }
            TaskStatus::FailedPermanent { error } => write!(f, "FailedPerananent(error={})", error),
        }
    }
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "Pending",
            TaskStatus::Assigned => "Assigned",
            TaskStatus::Running => "Running",
            TaskStatus::Completed => "Completed",
            TaskStatus::Failed { .. } => "Failed",
            TaskStatus::FailedPermanent { .. } => "FailedPermanent",
        }
    }

    pub fn mark_as_assigned(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Pending => Ok(TaskStatus::Assigned),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Assigned",
            }),
        }
    }

    pub fn mark_as_running(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Assigned => Ok(TaskStatus::Running),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Running",
            }),
        }
    }

    pub fn mark_as_completed(&self) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running => Ok(TaskStatus::Completed),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Completed",
            }),
        }
    }

    pub fn mark_as_failed(
        &self,
        attempt: u32,
        error: String,
    ) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running => Ok(TaskStatus::Failed { attempt, error }),
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "Failed",
            }),
        }
    }

    pub fn mark_as_failed_permanent(
        &self,
        error: String,
    ) -> Result<TaskStatus, TaskTransitionError> {
        match self {
            TaskStatus::Running | TaskStatus::Failed { .. } => {
                Ok(TaskStatus::FailedPermanent { error })
            }
            status => Err(TaskTransitionError::Illegal {
                from: status.as_str(),
                to: "FailedPermanent",
            }),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::FailedPermanent { .. }
        )
    }
}

impl RetryPolicy {
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder {
            ..Default::default()
        }
    }

    pub fn can_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_ms: 10_000, // 10 seconds
        }
    }
}

impl RetryPolicyBuilder {
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    pub fn backoff_ms(mut self, backoff_ms: u64) -> Self {
        self.backoff_ms = Some(backoff_ms);
        self
    }

    pub fn build(self) -> RetryPolicy {
        let default = RetryPolicy::default();
        RetryPolicy {
            max_retries: self.max_retries.unwrap_or(default.max_retries),
            backoff_ms: self.backoff_ms.unwrap_or(default.backoff_ms),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
    pub status: TaskStatus,
    pub retry_policy: RetryPolicy,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl Task {
    pub fn builder(payload: Vec<u8>) -> TaskBuilder {
        TaskBuilder {
            payload,
            ..Default::default()
        }
    }

    pub fn assign(&mut self) -> Result<(), TaskTransitionError> {
        self.status = self.status.mark_as_assigned()?;
        self.updated_at = SystemTime::now();
        Ok(())
    }

    pub fn start(&mut self) -> Result<(), TaskTransitionError> {
        self.status = self.status.mark_as_running()?;
        self.updated_at = SystemTime::now();
        Ok(())
    }

    pub fn complete(&mut self) -> Result<(), TaskTransitionError> {
        self.status = self.status.mark_as_completed()?;
        self.updated_at = SystemTime::now();
        Ok(())
    }

    pub fn fail_retry(&mut self, error: &str) -> Result<(), TaskTransitionError> {
        let attempt = match &self.status {
            TaskStatus::Running => 1,
            TaskStatus::Failed { attempt, .. } => attempt + 1,
            _ => {
                return Err(TaskTransitionError::Illegal {
                    from: self.status.as_str(),
                    to: "FailedRetry",
                });
            }
        };

        if attempt > self.retry_policy.max_retries {
            return Err(TaskTransitionError::RetryLimitedExceeded);
        }

        self.status = self.status.mark_as_failed(attempt, error.to_string())?;
        self.updated_at = SystemTime::now();
        Ok(())
    }

    pub fn fail_permanent(&mut self, error: &str) -> Result<(), TaskTransitionError> {
        self.status = self.status.mark_as_failed_permanent(error.to_string())?;
        self.updated_at = SystemTime::now();
        Ok(())
    }
}

impl Default for Task {
    fn default() -> Self {
        let now = SystemTime::now();
        Task {
            id: TaskId::new(),
            job_id: JobId::new(),
            status: TaskStatus::Pending,
            retry_policy: RetryPolicy::builder().build(),
            payload: vec![],
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskBuilder {
    pub id: Option<TaskId>,
    pub job_id: Option<JobId>,
    pub payload: Vec<u8>,
    pub status: Option<TaskStatus>,
    pub retry_policy: Option<RetryPolicy>,
    pub created_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
}

impl TaskBuilder {
    pub fn id(mut self, id: TaskId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn job_id(mut self, job_id: JobId) -> Self {
        self.job_id = Some(job_id);
        self
    }

    pub fn status(mut self, status: TaskStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    pub fn created_at(mut self, created_at: SystemTime) -> Self {
        self.created_at = Some(created_at);
        self
    }

    pub fn updated_at(mut self, updated_at: SystemTime) -> Self {
        self.updated_at = Some(updated_at);
        self
    }

    pub fn build(self) -> Task {
        let default = Task::default();
        Task {
            id: self.id.unwrap_or(default.id),
            job_id: self.job_id.unwrap_or(default.job_id),
            status: self.status.unwrap_or(default.status),
            payload: self.payload,
            retry_policy: self.retry_policy.unwrap_or(default.retry_policy),
            created_at: self.created_at.unwrap_or(default.created_at),
            updated_at: self.updated_at.unwrap_or(default.updated_at),
        }
    }
}
