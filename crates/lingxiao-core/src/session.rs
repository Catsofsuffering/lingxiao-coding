#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Created,
    Active,
    Interrupted,
    Completed,
    Failed,
    Deleted,
}

impl SessionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Deleted)
    }

    pub fn can_transition_to(self, to: Self) -> bool {
        use SessionStatus::*;
        matches!(
            (self, to),
            (Created, Active)
                | (Active, Interrupted)
                | (Interrupted, Active)
                | (Active, Completed)
                | (Active, Failed)
                | (Completed | Failed, Deleted)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_transitions() {
        assert!(SessionStatus::Created.can_transition_to(SessionStatus::Active));
        assert!(SessionStatus::Active.can_transition_to(SessionStatus::Completed));
        assert!(!SessionStatus::Completed.can_transition_to(SessionStatus::Active));
    }

    #[test]
    fn test_terminal_states() {
        assert!(SessionStatus::Completed.is_terminal());
        assert!(SessionStatus::Failed.is_terminal());
        assert!(SessionStatus::Deleted.is_terminal());
        assert!(!SessionStatus::Active.is_terminal());
    }
}
