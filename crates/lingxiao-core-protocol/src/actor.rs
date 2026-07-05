use serde::{Deserialize, Serialize};

/// Stable source-of-action classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    System,
    User,
    Agent,
    Tool,
    Sidecar,
    Runtime,
}

/// Identifies who or what originated a command or event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub kind: ActorKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl Actor {
    pub fn new(kind: ActorKind) -> Self {
        Self { kind, id: None }
    }

    pub fn with_id(kind: ActorKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: Some(id.into()),
        }
    }
}

impl std::fmt::Display for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.id {
            Some(id) => write!(f, "{:?}({})", self.kind, id),
            None => write!(f, "{:?}", self.kind),
        }
    }
}
