#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationMessage {
    pub id: u64,
    pub role: String,
    pub content: String,
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedContext {
    pub summary: String,
    pub retained_message_ids: Vec<u64>,
    pub original_message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveContextMessage {
    pub role: String,
    pub content: String,
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct ContextManager {
    messages: Vec<ConversationMessage>,
    next_id: u64,
}

impl ContextManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(
        &mut self,
        role: impl Into<String>,
        content: impl Into<String>,
        tool_call_id: Option<String>,
    ) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.messages.push(ConversationMessage {
            id,
            role: role.into(),
            content: content.into(),
            tool_call_id,
        });
        id
    }

    pub fn replay(&self) -> &[ConversationMessage] {
        &self.messages
    }

    pub fn compact(&self, retain_last: usize) -> CompactedContext {
        let split_at = self.messages.len().saturating_sub(retain_last);
        let compacted = &self.messages[..split_at];
        let retained = &self.messages[split_at..];
        let summary = if compacted.is_empty() {
            String::new()
        } else {
            compacted
                .iter()
                .map(|message| format!("{}: {}", message.role, message.content))
                .collect::<Vec<_>>()
                .join("\n")
        };
        CompactedContext {
            summary,
            retained_message_ids: retained.iter().map(|message| message.id).collect(),
            original_message_count: self.messages.len(),
        }
    }

    pub fn active_projection(&self, retain_last: usize) -> Vec<ActiveContextMessage> {
        let compacted = self.compact(retain_last);
        let retained_ids = compacted.retained_message_ids;
        let retained_start = self
            .messages
            .iter()
            .position(|message| retained_ids.contains(&message.id))
            .unwrap_or(self.messages.len());
        let mut projection = Vec::new();
        if !compacted.summary.is_empty() {
            projection.push(ActiveContextMessage {
                role: "system".into(),
                content: format!("Previous conversation summary:\n{}", compacted.summary),
                tool_call_id: None,
            });
        }
        projection.extend(self.messages[retained_start..].iter().map(|message| {
            ActiveContextMessage {
                role: message.role.clone(),
                content: message.content.clone(),
                tool_call_id: message.tool_call_id.clone(),
            }
        }));
        projection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_manager_creation() {
        let cm = ContextManager::new();
        assert!(cm.replay().is_empty());
    }

    #[test]
    fn test_gs033_message_replay_preserves_order() {
        let mut cm = ContextManager::new();
        cm.append("user", "first", None);
        cm.append("assistant", "second", None);
        cm.append("tool", "third", Some("tool-call-1".into()));

        let replay = cm.replay();
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0].content, "first");
        assert_eq!(replay[1].content, "second");
        assert_eq!(replay[2].tool_call_id.as_deref(), Some("tool-call-1"));
    }

    #[test]
    fn test_gs034_compaction_does_not_delete_original_facts() {
        let mut cm = ContextManager::new();
        cm.append("user", "fact A", None);
        cm.append("assistant", "fact B", None);
        cm.append("user", "recent C", None);

        let compacted = cm.compact(1);
        assert!(compacted.summary.contains("fact A"));
        assert!(compacted.summary.contains("fact B"));
        assert_eq!(compacted.retained_message_ids, vec![3]);
        assert_eq!(compacted.original_message_count, 3);

        let replay = cm.replay();
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0].content, "fact A");
        assert_eq!(replay[1].content, "fact B");
        assert_eq!(replay[2].content, "recent C");
    }

    #[test]
    fn test_active_projection_reduces_window_and_preserves_facts_in_summary() {
        let mut cm = ContextManager::new();
        cm.append("user", "fact A", None);
        cm.append("assistant", "fact B", None);
        cm.append("user", "recent C", None);

        let projection = cm.active_projection(1);
        assert_eq!(projection.len(), 2);
        assert_eq!(projection[0].role, "system");
        assert!(projection[0].content.contains("fact A"));
        assert!(projection[0].content.contains("fact B"));
        assert_eq!(projection[1].content, "recent C");
        assert_eq!(cm.replay().len(), 3);
    }
}
