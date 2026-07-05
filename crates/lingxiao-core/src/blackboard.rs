pub struct Blackboard;

impl Blackboard {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Blackboard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blackboard_creation() {
        let _b = Blackboard::new();
    }
}
