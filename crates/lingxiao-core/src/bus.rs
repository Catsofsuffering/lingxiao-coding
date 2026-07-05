use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    P0,
    P1,
    P2,
    P3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub from: String,
    pub to: String,
    pub priority: Priority,
    pub content: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    pub message: Message,
    pub reason: String,
}

pub struct MessageBus {
    capacity: usize,
    queues: [VecDeque<Message>; 4],
    dead_letters: Vec<DeadLetter>,
    dead_letter_capacity: usize,
}

impl MessageBus {
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            queues: std::array::from_fn(|_| VecDeque::new()),
            dead_letters: Vec::new(),
            dead_letter_capacity: capacity.max(1),
        }
    }

    pub fn publish(&mut self, message: Message) -> Result<(), DeadLetter> {
        if self.len() >= self.capacity {
            let dead = DeadLetter {
                message,
                reason: "backpressure_capacity_exceeded".into(),
            };
            self.push_dead_letter(dead.clone());
            return Err(dead);
        }
        self.queues[priority_index(message.priority)].push_back(message);
        Ok(())
    }

    pub fn pop_next(&mut self) -> Option<Message> {
        for queue in &mut self.queues {
            if let Some(message) = queue.pop_front() {
                return Some(message);
            }
        }
        None
    }

    pub fn reject(&mut self, message: Message, reason: impl Into<String>) {
        self.push_dead_letter(DeadLetter {
            message,
            reason: reason.into(),
        });
    }

    pub fn len(&self) -> usize {
        self.queues.iter().map(VecDeque::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dead_letters(&self) -> &[DeadLetter] {
        &self.dead_letters
    }

    fn push_dead_letter(&mut self, dead: DeadLetter) {
        if self.dead_letters.len() >= self.dead_letter_capacity {
            self.dead_letters.remove(0);
        }
        self.dead_letters.push(dead);
    }
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new()
    }
}

fn priority_index(priority: Priority) -> usize {
    match priority {
        Priority::P0 => 0,
        Priority::P1 => 1,
        Priority::P2 => 2,
        Priority::P3 => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, priority: Priority) -> Message {
        Message {
            from: "leader".into(),
            to: "agent".into(),
            priority,
            content: id.as_bytes().to_vec(),
        }
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::P0 < Priority::P1);
        assert!(Priority::P1 < Priority::P2);
        assert!(Priority::P2 < Priority::P3);
    }

    #[test]
    fn test_gs031_priority_delivery() {
        let mut bus = MessageBus::with_capacity(10);
        bus.publish(message("p3-first", Priority::P3)).unwrap();
        bus.publish(message("p1-second", Priority::P1)).unwrap();
        bus.publish(message("p0-third", Priority::P0)).unwrap();
        bus.publish(message("p2-fourth", Priority::P2)).unwrap();

        assert_eq!(bus.pop_next().unwrap().content, b"p0-third");
        assert_eq!(bus.pop_next().unwrap().content, b"p1-second");
        assert_eq!(bus.pop_next().unwrap().content, b"p2-fourth");
        assert_eq!(bus.pop_next().unwrap().content, b"p3-first");
        assert!(bus.is_empty());
    }

    #[test]
    fn test_gs031_backpressure_to_dead_letter() {
        let mut bus = MessageBus::with_capacity(1);
        bus.publish(message("first", Priority::P1)).unwrap();
        let err = bus.publish(message("overflow", Priority::P0)).unwrap_err();

        assert_eq!(err.reason, "backpressure_capacity_exceeded");
        assert_eq!(bus.dead_letters().len(), 1);
        assert_eq!(bus.dead_letters()[0].message.content, b"overflow");
    }

    #[test]
    fn test_gs032_explicit_dead_letter() {
        let mut bus = MessageBus::new();
        bus.reject(message("bad", Priority::P0), "handler_failed");

        assert_eq!(bus.dead_letters().len(), 1);
        assert_eq!(bus.dead_letters()[0].reason, "handler_failed");
        assert_eq!(bus.dead_letters()[0].message.content, b"bad");
    }

    #[test]
    fn test_dead_letters_are_bounded() {
        let mut bus = MessageBus::with_capacity(2);
        bus.reject(message("old", Priority::P0), "failed");
        bus.reject(message("middle", Priority::P0), "failed");
        bus.reject(message("new", Priority::P0), "failed");

        assert_eq!(bus.dead_letters().len(), 2);
        assert_eq!(bus.dead_letters()[0].message.content, b"middle");
        assert_eq!(bus.dead_letters()[1].message.content, b"new");
    }
}
