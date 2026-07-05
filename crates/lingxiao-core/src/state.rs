pub trait StateMachine: Sized {
    type State: Copy + PartialEq + Eq + std::fmt::Debug;
    type Event;

    fn current_state(&self) -> Self::State;

    fn apply(&mut self, event: Self::Event) -> Result<(), TransitionError>;

    fn can_transition(from: Self::State, to: Self::State) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransitionError {
    #[error("Invalid transition: {from:?} -> {to:?}")]
    Invalid { from: String, to: String },

    #[error("Terminal state: {state:?}")]
    Terminal { state: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transition_error_display() {
        let err = TransitionError::Invalid {
            from: "a".into(),
            to: "b".into(),
        };
        assert_eq!(format!("{err}"), "Invalid transition: \"a\" -> \"b\"");
    }
}
