pub mod agent;
pub mod blackboard;
pub mod bus;
pub mod command;
pub mod context;
pub mod document_tools;
pub mod event_log;
pub mod leader;
pub mod llm;
pub mod mcp_bridge;
pub mod permission;
pub mod persistence;
pub mod process;
pub mod projection;
pub mod repl;
pub mod runtime;
pub mod schedule;
pub mod session;
pub mod sidecar;
pub mod state;
pub mod task;
pub mod terminal;
pub mod tool;
pub mod workflow;

pub use lingxiao_core_protocol as protocol;
pub use lingxiao_llm_host_protocol as llm_protocol;
pub use lingxiao_tool_host_protocol as tool_protocol;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = "lingxiao-core";

pub fn core_info() -> String {
    format!("{NAME} v{VERSION}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_info() {
        let info = core_info();
        assert!(info.contains(NAME));
        assert!(info.contains(VERSION));
    }

    #[test]
    fn test_version_constants() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
        assert_eq!(NAME, "lingxiao-core");
    }
}
