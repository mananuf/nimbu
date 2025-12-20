// task.rs
use std::{fmt::Display, time::SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{JobId, TaskId};

/// Execution outcome reported by workers
#[derive(Debug)]
pub enum TaskResult {
    Success,
    RetryableFailure(String),
    FatalFailure(String),
}

/// Backoff strategy by the scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed(std::time::Duration),
    Exponential {
        base: std::time::Duration,
        factor: u32,
        max_delay: std::time::Duration,
    },
}

/// Retry policy interpreted exclusively by the scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub strategy: BackoffStrategy,
}

impl RetryPolicy {
    pub fn can_retry(&self, attempts: u32) -> bool {
        attempts < self.max_retries
    }
}

/// Task lifecycle states
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed { attempts: u32, error: String },
    FailedPermanent { error: String },
}

impl Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "Pending"),
            TaskStatus::Assigned => write!(f, "Assigned"),
            TaskStatus::Running => write!(f, "Running"),
            TaskStatus::Completed => write!(f, "Completed"),
            TaskStatus::Failed { attempts, error } => {
                write!(f, "Failed(attempts={}, error={})", attempts, error)
            }
            TaskStatus::FailedPermanent { error } => {
                write!(f, "FailedPermanent(error={})", error)
            }
        }
    }
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::FailedPermanent { .. }
        )
    }
}

/// Task domain object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub payload: Vec<u8>,

    pub status: TaskStatus,
    pub attempts: u32,
    pub retry_policy: Option<RetryPolicy>,

    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskBuilder {
    pub id: Option<TaskId>,
    pub job_id: Option<JobId>,
    pub payload: Vec<u8>,

    pub status: Option<TaskStatus>,
    pub attempts: Option<u32>,
    pub retry_policy: Option<RetryPolicy>,

    pub created_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
}

#[derive(Debug, Error)]
pub enum TaskTransitionError {
    #[error("illegal task transition from {from} to {to}")]
    Illegal {
        from: &'static str,
        to: &'static str,
    },
}

impl Task {
    pub fn new(payload: Vec<u8>) -> TaskBuilder {
        TaskBuilder {
            payload,
            ..Default::default()
        }
    }

    pub fn assign(&mut self) -> Result<(), TaskTransitionError> {
        match self.status {
            TaskStatus::Pending => {
                self.status = TaskStatus::Assigned;
                self.updated_at = SystemTime::now();
                Ok(())
            }
            _ => Err(TaskTransitionError::Illegal {
                from: self.status.as_str(),
                to: "Assigned",
            }),
        }
    }

    pub fn start(&mut self) -> Result<(), TaskTransitionError> {
        match self.status {
            TaskStatus::Assigned => {
                self.status = TaskStatus::Running;
                self.updated_at = SystemTime::now();
                Ok(())
            }
            _ => Err(TaskTransitionError::Illegal {
                from: self.status.as_str(),
                to: "Running",
            }),
        }
    }

    pub fn complete(&mut self) -> Result<(), TaskTransitionError> {
        match self.status {
            TaskStatus::Running => {
                self.status = TaskStatus::Completed;
                self.updated_at = SystemTime::now();
                Ok(())
            }
            _ => Err(TaskTransitionError::Illegal {
                from: self.status.as_str(),
                to: "Completed",
            }),
        }
    }

    pub fn mark_retryable_failure(&mut self, error: String) {
        self.attempts += 1;
        self.status = TaskStatus::Failed {
            attempts: self.attempts,
            error,
        };
        self.updated_at = SystemTime::now();
    }

    pub fn mark_permanent_failure(&mut self, error: String) {
        self.status = TaskStatus::FailedPermanent { error };
        self.updated_at = SystemTime::now();
    }
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

    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = Some(attempts);
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
        let now = SystemTime::now();
        Task {
            id: self.id.unwrap_or_default(),
            job_id: self.job_id.unwrap_or_default(),
            payload: self.payload,
            status: self.status.unwrap_or(TaskStatus::Pending),
            attempts: self.attempts.unwrap_or_default(),
            retry_policy: self.retry_policy,
            created_at: self.created_at.unwrap_or(now),
            updated_at: self.created_at.unwrap_or(now),
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
}
