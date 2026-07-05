use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderTaskDraft {
    pub task_id: Option<String>,
    pub subject: String,
    pub description: String,
}

impl LeaderTaskDraft {
    pub fn to_value(&self) -> Value {
        json!({
            "task_id": self.task_id,
            "subject": self.subject,
            "description": self.description,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderPlan {
    pub tasks: Vec<LeaderTaskDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderPlanError {
    EmptyObjective,
    InvalidTasks(String),
}

pub struct LeaderOrchestrator {
    max_tasks: usize,
}

impl LeaderOrchestrator {
    pub fn new() -> Self {
        Self { max_tasks: 20 }
    }

    pub fn with_max_tasks(mut self, max_tasks: usize) -> Self {
        self.max_tasks = max_tasks.max(1);
        self
    }

    pub fn plan_from_objective(&self, objective: &str) -> Result<LeaderPlan, LeaderPlanError> {
        let objective = objective.trim();
        if objective.is_empty() {
            return Err(LeaderPlanError::EmptyObjective);
        }
        Ok(LeaderPlan {
            tasks: vec![LeaderTaskDraft {
                task_id: None,
                subject: first_sentence_or_prefix(objective, 80),
                description: objective.to_string(),
            }],
        })
    }

    pub fn normalize_task_values(&self, tasks: &[Value]) -> Result<LeaderPlan, LeaderPlanError> {
        let mut drafts = Vec::new();
        for task in tasks.iter().take(self.max_tasks) {
            let subject = task
                .get("subject")
                .and_then(|v| v.as_str())
                .or_else(|| task.get("title").and_then(|v| v.as_str()))
                .unwrap_or("Leader task")
                .trim()
                .to_string();
            if subject.is_empty() {
                return Err(LeaderPlanError::InvalidTasks(
                    "task subject must not be empty".into(),
                ));
            }
            let description = task
                .get("description")
                .and_then(|v| v.as_str())
                .or_else(|| task.get("body").and_then(|v| v.as_str()))
                .unwrap_or("")
                .trim()
                .to_string();
            let task_id = task
                .get("task_id")
                .or_else(|| task.get("id"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string);
            drafts.push(LeaderTaskDraft {
                task_id,
                subject,
                description,
            });
        }
        Ok(LeaderPlan { tasks: drafts })
    }
}

impl Default for LeaderOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

fn first_sentence_or_prefix(text: &str, max_chars: usize) -> String {
    let sentence = text.split(['.', '\n']).next().unwrap_or(text).trim();
    let mut subject = String::new();
    for ch in sentence.chars().take(max_chars) {
        subject.push(ch);
    }
    if subject.is_empty() {
        "Leader task".into()
    } else {
        subject
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_creation() {
        let _leader = LeaderOrchestrator::new();
    }

    #[test]
    fn test_p4_leader_plans_from_objective() {
        let plan = LeaderOrchestrator::new()
            .plan_from_objective("Implement durable workflow execution. Then verify it.")
            .unwrap();
        assert_eq!(plan.tasks.len(), 1);
        assert_eq!(
            plan.tasks[0].subject,
            "Implement durable workflow execution"
        );
    }

    #[test]
    fn test_p4_leader_normalizes_task_values() {
        let plan = LeaderOrchestrator::new()
            .normalize_task_values(&[
                json!({"id": "t1", "title": "Build", "body": "Do it"}),
                json!({"task_id": "t2", "subject": "Verify", "description": "Test it"}),
            ])
            .unwrap();
        assert_eq!(plan.tasks.len(), 2);
        assert_eq!(plan.tasks[0].task_id.as_deref(), Some("t1"));
        assert_eq!(plan.tasks[0].subject, "Build");
        assert_eq!(plan.tasks[1].description, "Test it");
    }
}
