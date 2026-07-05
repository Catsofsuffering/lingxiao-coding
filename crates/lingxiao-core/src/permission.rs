#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Strict,
    Dev,
    Networked,
    Yolo,
}

#[derive(Debug, Clone)]
pub struct PermissionRequest {
    pub request_id: String,
    pub tool_name: String,
    pub mode: Mode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mode_default() {
        let req = PermissionRequest {
            request_id: "test".into(),
            tool_name: "shell".into(),
            mode: Mode::Strict,
        };
        assert_eq!(req.mode, Mode::Strict);
    }
}
