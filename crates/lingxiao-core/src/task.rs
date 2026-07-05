#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Dispatchable,
    Blocked,
    Running,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExitReason {
    Completed,
    Failed,
    Cancelled,
    Timeout,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        self == Self::Terminal
    }

    pub fn can_transition_to(self, to: Self) -> bool {
        use TaskStatus::*;
        matches!(
            (self, to),
            (Blocked, Dispatchable | Terminal)
                | (Dispatchable, Running | Blocked | Terminal)
                | (Running, Terminal | Dispatchable | Blocked)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_transitions() {
        assert!(TaskStatus::Dispatchable.can_transition_to(TaskStatus::Running));
        assert!(TaskStatus::Blocked.can_transition_to(TaskStatus::Dispatchable));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Blocked));
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Terminal));
        assert!(!TaskStatus::Terminal.can_transition_to(TaskStatus::Running));
    }

    #[test]
    fn test_redispatch() {
        assert!(TaskStatus::Running.can_transition_to(TaskStatus::Dispatchable));
    }
}
