use crate::types::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallDelta {
    pub index: u32,
    pub id: Option<String>,
    pub name: Option<String>,
    pub partial_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FinishReason {
    Stop,
    ToolCalls,
    Length,
    ContentFiltered,
    Error,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamEvent {
    /// Non-durable realtime delta — LLM text content increment.
    TextDelta(String),
    /// Non-durable realtime delta — thinking/reasoning content increment.
    ThinkingDelta(String),
    /// Non-durable realtime delta — tool call parameter increment.
    ToolCallDelta(ToolCallDelta),
    /// Complete tool call (accumulated from deltas).
    ToolCall(ToolCall),
    /// Final token usage (emitted once at end of stream).
    Usage(TokenUsage),
    /// Stream finished with a reason.
    Finished(FinishReason),
    /// Provider-level error during streaming.
    Error(crate::ProviderError),
}

#[derive(Debug, Clone, Default)]
pub struct ToolCallBuilder {
    pub id: Option<String>,
    pub name: Option<String>,
    pub raw_json: String,
}

#[derive(Debug, Clone, Default)]
pub struct ToolCallAccumulator {
    calls: Vec<ToolCallBuilder>,
}

impl ToolCallAccumulator {
    pub fn new() -> Self {
        Self { calls: Vec::new() }
    }

    pub fn append(&mut self, delta: ToolCallDelta) {
        let index = delta.index as usize;
        while self.calls.len() <= index {
            self.calls.push(ToolCallBuilder::default());
        }
        if let Some(id) = delta.id {
            self.calls[index].id = Some(id);
        }
        if let Some(name) = delta.name {
            self.calls[index].name = Some(name);
        }
        if let Some(partial) = delta.partial_json {
            self.calls[index].raw_json.push_str(&partial);
        }
    }

    pub fn finalize(&mut self) -> Vec<ToolCall> {
        let mut result = Vec::new();
        let mut remaining = Vec::new();
        std::mem::swap(&mut remaining, &mut self.calls);
        for builder in remaining {
            let Some(id) = builder.id.filter(|value| !value.trim().is_empty()) else {
                continue;
            };
            let Some(name) = builder.name.filter(|value| !value.trim().is_empty()) else {
                continue;
            };
            let arguments: serde_json::Value =
                serde_json::from_str(&builder.raw_json).unwrap_or_else(|_| serde_json::json!({}));
            result.push(ToolCall {
                id,
                name,
                arguments,
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_call_accumulator_merges_partial_args() {
        let mut acc = ToolCallAccumulator::new();

        acc.append(ToolCallDelta {
            index: 0,
            id: Some("call_1".into()),
            name: Some("get_weather".into()),
            partial_json: Some(r#"{"loc"#.into()),
        });
        acc.append(ToolCallDelta {
            index: 0,
            id: None,
            name: None,
            partial_json: Some(r#"ation":"Shanghai"}"#.into()),
        });

        let tools = acc.finalize();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, "call_1");
        assert_eq!(tools[0].name, "get_weather");
        assert_eq!(
            tools[0].arguments,
            serde_json::json!({"location": "Shanghai"})
        );
    }

    #[test]
    fn test_tool_call_accumulator_multiple_tools() {
        let mut acc = ToolCallAccumulator::new();

        acc.append(ToolCallDelta {
            index: 0,
            id: Some("call_1".into()),
            name: Some("search".into()),
            partial_json: Some(r#"{"query":"hello"}"#.into()),
        });
        acc.append(ToolCallDelta {
            index: 1,
            id: Some("call_2".into()),
            name: Some("read_file".into()),
            partial_json: Some(r#"{"path":"/tmp"}"#.into()),
        });

        let tools = acc.finalize();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0].name, "search");
        assert_eq!(tools[1].name, "read_file");
    }

    #[test]
    fn test_tool_call_accumulator_empty() {
        let mut acc = ToolCallAccumulator::new();
        let tools = acc.finalize();
        assert!(tools.is_empty());
    }

    #[test]
    fn test_tool_call_accumulator_drops_incomplete_builders() {
        let mut acc = ToolCallAccumulator::new();

        acc.append(ToolCallDelta {
            index: 0,
            id: Some("call_1".into()),
            name: Some("file_read".into()),
            partial_json: Some(r#"{"path":"ok"}"#.into()),
        });
        acc.append(ToolCallDelta {
            index: 1,
            id: None,
            name: None,
            partial_json: Some(r#"{"path":"ignored"}"#.into()),
        });
        acc.append(ToolCallDelta {
            index: 2,
            id: Some("call_3".into()),
            name: Some("".into()),
            partial_json: Some(r#"{"path":"ignored"}"#.into()),
        });

        let tools = acc.finalize();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, "call_1");
        assert_eq!(tools[0].name, "file_read");
    }

    #[test]
    fn test_stream_event_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<StreamEvent>();
        assert_sync::<StreamEvent>();
    }

    #[test]
    fn test_finish_reason_serde() {
        let reasons = [
            FinishReason::Stop,
            FinishReason::ToolCalls,
            FinishReason::Length,
            FinishReason::ContentFiltered,
            FinishReason::Error,
            FinishReason::Unknown,
        ];
        for r in &reasons {
            let json = serde_json::to_string(r).unwrap();
            let back: FinishReason = serde_json::from_str(&json).unwrap();
            assert_eq!(*r, back);
        }
    }
}
