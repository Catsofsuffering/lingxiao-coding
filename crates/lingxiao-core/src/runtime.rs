use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Sidecar,
    Tool,
    Worker,
    Memory,
    Token,
    FileWrite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeBudget {
    pub max_sidecars: u32,
    pub max_workers: u32,
    pub max_memory_mb: u64,
    pub max_tokens: u64,
    pub max_file_write_bytes: u64,
    pub max_tool_concurrency: u32,
}

impl Default for RuntimeBudget {
    fn default() -> Self {
        Self {
            max_sidecars: 3,
            max_workers: 8,
            max_memory_mb: 512,
            max_tokens: 100_000,
            max_file_write_bytes: 50 * 1024 * 1024,
            max_tool_concurrency: 32,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeUsage {
    pub active_sidecars: u32,
    pub active_workers: u32,
    pub active_tools: u32,
    pub memory_mb: u64,
    pub tokens: u64,
    pub file_write_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetExceeded {
    pub resource: ResourceKind,
    pub requested: u64,
    pub available: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarReservation {
    pub token: String,
    pub memory_mb: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeManager {
    budget: RuntimeBudget,
    usage: RuntimeUsage,
    sidecars: HashMap<String, SidecarReservation>,
    workers: HashMap<String, ()>,
    tools: HashMap<String, ()>,
    token_reservations: HashMap<String, u64>,
    file_write_reservations: HashMap<String, u64>,
    agent_token_usage: HashMap<String, u64>,
    agent_token_budgets: HashMap<String, u64>,
    agent_token_reservations: HashMap<String, (String, u64)>,
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self::with_budget(RuntimeBudget::default())
    }

    pub fn with_budget(budget: RuntimeBudget) -> Self {
        Self {
            budget,
            usage: RuntimeUsage::default(),
            sidecars: HashMap::new(),
            workers: HashMap::new(),
            tools: HashMap::new(),
            token_reservations: HashMap::new(),
            file_write_reservations: HashMap::new(),
            agent_token_usage: HashMap::new(),
            agent_token_budgets: HashMap::new(),
            agent_token_reservations: HashMap::new(),
        }
    }

    pub fn usage(&self) -> &RuntimeUsage {
        &self.usage
    }

    pub fn budget(&self) -> &RuntimeBudget {
        &self.budget
    }

    pub fn try_acquire_sidecar(
        &mut self,
        token: impl Into<String>,
        memory_mb: u64,
    ) -> Result<SidecarReservation, BudgetExceeded> {
        if self.usage.active_sidecars >= self.budget.max_sidecars {
            return Err(BudgetExceeded {
                resource: ResourceKind::Sidecar,
                requested: self.usage.active_sidecars as u64 + 1,
                available: self.budget.max_sidecars as u64,
            });
        }
        if self.usage.memory_mb + memory_mb > self.budget.max_memory_mb {
            return Err(BudgetExceeded {
                resource: ResourceKind::Memory,
                requested: self.usage.memory_mb + memory_mb,
                available: self.budget.max_memory_mb,
            });
        }
        let reservation = SidecarReservation {
            token: token.into(),
            memory_mb,
        };
        self.usage.active_sidecars += 1;
        self.usage.memory_mb += memory_mb;
        self.sidecars
            .insert(reservation.token.clone(), reservation.clone());
        Ok(reservation)
    }

    pub fn release_sidecar(&mut self, token: &str) -> bool {
        let Some(reservation) = self.sidecars.remove(token) else {
            return false;
        };
        self.usage.active_sidecars = self.usage.active_sidecars.saturating_sub(1);
        self.usage.memory_mb = self.usage.memory_mb.saturating_sub(reservation.memory_mb);
        true
    }

    pub fn try_acquire_worker(
        &mut self,
        token: impl Into<String>,
    ) -> Result<String, BudgetExceeded> {
        let token = token.into();
        if self.workers.contains_key(&token) {
            return Ok(token);
        }
        if self.usage.active_workers >= self.budget.max_workers {
            return Err(BudgetExceeded {
                resource: ResourceKind::Worker,
                requested: self.usage.active_workers as u64 + 1,
                available: self.budget.max_workers as u64,
            });
        }
        self.usage.active_workers += 1;
        self.workers.insert(token.clone(), ());
        Ok(token)
    }

    pub fn release_worker(&mut self, token: &str) -> bool {
        if self.workers.remove(token).is_none() {
            return false;
        }
        self.usage.active_workers = self.usage.active_workers.saturating_sub(1);
        true
    }

    pub fn try_acquire_tool(&mut self, token: impl Into<String>) -> Result<String, BudgetExceeded> {
        if self.usage.active_tools >= self.budget.max_tool_concurrency {
            return Err(BudgetExceeded {
                resource: ResourceKind::Tool,
                requested: self.usage.active_tools as u64 + 1,
                available: self.budget.max_tool_concurrency as u64,
            });
        }
        let token = token.into();
        self.usage.active_tools += 1;
        self.tools.insert(token.clone(), ());
        Ok(token)
    }

    pub fn release_tool(&mut self, token: &str) -> bool {
        if self.tools.remove(token).is_none() {
            return false;
        }
        self.usage.active_tools = self.usage.active_tools.saturating_sub(1);
        true
    }

    pub fn check_token_budget(&self, current: u64, next_cost: u64) -> Result<(), BudgetExceeded> {
        let reserved = self.reserved_tokens();
        if current + reserved + next_cost > self.budget.max_tokens {
            Err(BudgetExceeded {
                resource: ResourceKind::Token,
                requested: current + reserved + next_cost,
                available: self.budget.max_tokens,
            })
        } else {
            Ok(())
        }
    }

    pub fn try_reserve_tokens(
        &mut self,
        token: impl Into<String>,
        tokens: u64,
    ) -> Result<String, BudgetExceeded> {
        let token = token.into();
        self.check_token_budget(self.usage.tokens, tokens)?;
        self.token_reservations.insert(token.clone(), tokens);
        Ok(token)
    }

    pub fn release_token_reservation(&mut self, token: &str) -> bool {
        self.token_reservations.remove(token).is_some()
    }

    pub fn commit_token_reservation(&mut self, token: &str) -> bool {
        let Some(tokens) = self.token_reservations.remove(token) else {
            return false;
        };
        self.commit_tokens(tokens);
        true
    }

    pub fn commit_token_reservation_actual(&mut self, token: &str, actual_tokens: u64) -> bool {
        if self.token_reservations.remove(token).is_none() {
            return false;
        }
        self.commit_tokens(actual_tokens);
        true
    }

    pub fn set_agent_token_budget(&mut self, agent_id: impl Into<String>, max_tokens: u64) {
        self.agent_token_budgets.insert(agent_id.into(), max_tokens);
    }

    pub fn agent_token_usage(&self, agent_id: &str) -> u64 {
        self.agent_token_usage.get(agent_id).copied().unwrap_or(0)
    }

    pub fn check_agent_token_budget(
        &self,
        agent_id: &str,
        next_cost: u64,
    ) -> Result<(), BudgetExceeded> {
        let Some(max_tokens) = self.agent_token_budgets.get(agent_id).copied() else {
            return Ok(());
        };
        let current = self.agent_token_usage(agent_id);
        let reserved = self.reserved_agent_tokens(agent_id);
        if current + reserved + next_cost > max_tokens {
            Err(BudgetExceeded {
                resource: ResourceKind::Token,
                requested: current + reserved + next_cost,
                available: max_tokens,
            })
        } else {
            Ok(())
        }
    }

    pub fn try_reserve_agent_tokens(
        &mut self,
        token: impl Into<String>,
        agent_id: &str,
        tokens: u64,
    ) -> Result<String, BudgetExceeded> {
        let token = token.into();
        self.check_agent_token_budget(agent_id, tokens)?;
        if !agent_id.is_empty() && tokens > 0 {
            self.agent_token_reservations
                .insert(token.clone(), (agent_id.to_string(), tokens));
        }
        Ok(token)
    }

    pub fn release_agent_token_reservation(&mut self, token: &str) -> bool {
        self.agent_token_reservations.remove(token).is_some()
    }

    pub fn commit_agent_token_reservation(&mut self, token: &str) -> bool {
        let Some((agent_id, tokens)) = self.agent_token_reservations.remove(token) else {
            return false;
        };
        self.commit_agent_tokens(&agent_id, tokens);
        true
    }

    pub fn commit_agent_token_reservation_actual(
        &mut self,
        token: &str,
        actual_tokens: u64,
    ) -> bool {
        let Some((agent_id, _)) = self.agent_token_reservations.remove(token) else {
            return false;
        };
        self.commit_agent_tokens(&agent_id, actual_tokens);
        true
    }

    pub fn check_file_write(&self, bytes: u64) -> Result<(), BudgetExceeded> {
        let reserved = self.reserved_file_write_bytes();
        if self.usage.file_write_bytes + reserved + bytes > self.budget.max_file_write_bytes {
            Err(BudgetExceeded {
                resource: ResourceKind::FileWrite,
                requested: self.usage.file_write_bytes + reserved + bytes,
                available: self.budget.max_file_write_bytes,
            })
        } else {
            Ok(())
        }
    }

    pub fn try_reserve_file_write(
        &mut self,
        token: impl Into<String>,
        bytes: u64,
    ) -> Result<String, BudgetExceeded> {
        let token = token.into();
        self.check_file_write(bytes)?;
        self.file_write_reservations.insert(token.clone(), bytes);
        Ok(token)
    }

    pub fn release_file_write_reservation(&mut self, token: &str) -> bool {
        self.file_write_reservations.remove(token).is_some()
    }

    pub fn commit_file_write_reservation(&mut self, token: &str) -> bool {
        let Some(bytes) = self.file_write_reservations.remove(token) else {
            return false;
        };
        self.commit_file_write(bytes);
        true
    }

    pub fn commit_tokens(&mut self, tokens: u64) {
        self.usage.tokens = self.usage.tokens.saturating_add(tokens);
    }

    pub fn commit_agent_tokens(&mut self, agent_id: &str, tokens: u64) {
        if agent_id.is_empty() {
            return;
        }
        let current = self.agent_token_usage(agent_id);
        self.agent_token_usage
            .insert(agent_id.to_string(), current.saturating_add(tokens));
    }

    pub fn commit_file_write(&mut self, bytes: u64) {
        self.usage.file_write_bytes = self.usage.file_write_bytes.saturating_add(bytes);
    }

    fn reserved_tokens(&self) -> u64 {
        self.token_reservations.values().copied().sum()
    }

    fn reserved_agent_tokens(&self, agent_id: &str) -> u64 {
        self.agent_token_reservations
            .values()
            .filter_map(|(reserved_agent_id, tokens)| {
                (reserved_agent_id == agent_id).then_some(*tokens)
            })
            .sum()
    }

    fn reserved_file_write_bytes(&self) -> u64 {
        self.file_write_reservations.values().copied().sum()
    }
}

impl Default for RuntimeManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_creation() {
        let rm = RuntimeManager::new();
        assert_eq!(rm.budget().max_sidecars, 3);
        assert_eq!(rm.usage().active_sidecars, 0);
    }

    #[test]
    fn test_gs030_sidecar_acquire_and_release_budget() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 2,
            max_workers: 2,
            max_memory_mb: 300,
            max_tokens: 100,
            max_file_write_bytes: 1024,
            max_tool_concurrency: 4,
        });
        runtime.try_acquire_sidecar("a", 100).unwrap();
        runtime.try_acquire_sidecar("b", 150).unwrap();

        let err = runtime.try_acquire_sidecar("c", 10).unwrap_err();
        assert_eq!(err.resource, ResourceKind::Sidecar);
        assert_eq!(runtime.usage().active_sidecars, 2);

        assert!(runtime.release_sidecar("a"));
        assert_eq!(runtime.usage().active_sidecars, 1);
        assert_eq!(runtime.usage().memory_mb, 150);
        assert!(runtime.try_acquire_sidecar("c", 10).is_ok());
    }

    #[test]
    fn test_gs029_token_budget_check() {
        let runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 1024,
            max_tool_concurrency: 4,
        });
        assert!(runtime.check_token_budget(80, 10).is_ok());
        let err = runtime.check_token_budget(95, 10).unwrap_err();
        assert_eq!(err.requested, 105);
        assert_eq!(err.available, 100);
    }

    #[test]
    fn test_token_reservation_prevents_concurrent_overspend() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 1024,
            max_tool_concurrency: 4,
        });
        assert_eq!(runtime.try_reserve_tokens("a", 80).unwrap(), "a");
        let err = runtime.try_reserve_tokens("b", 30).unwrap_err();
        assert_eq!(err.resource, ResourceKind::Token);
        assert!(runtime.release_token_reservation("a"));
        assert!(runtime.try_reserve_tokens("b", 30).is_ok());
        assert!(runtime.commit_token_reservation("b"));
        assert_eq!(runtime.usage().tokens, 30);
    }

    #[test]
    fn test_agent_token_budget_check_and_commit() {
        let mut runtime = RuntimeManager::new();
        runtime.set_agent_token_budget("agent-1", 10);
        runtime.commit_agent_tokens("agent-1", 7);
        assert!(runtime.check_agent_token_budget("agent-1", 3).is_ok());
        let err = runtime.check_agent_token_budget("agent-1", 4).unwrap_err();
        assert_eq!(err.resource, ResourceKind::Token);
        assert_eq!(err.requested, 11);
        assert_eq!(err.available, 10);
        assert!(runtime.check_agent_token_budget("agent-2", 1_000).is_ok());
    }

    #[test]
    fn test_empty_agent_does_not_record_agent_tokens() {
        let mut runtime = RuntimeManager::new();
        runtime.commit_agent_tokens("", 42);
        assert_eq!(runtime.agent_token_usage(""), 0);
    }

    #[test]
    fn test_file_write_budget_check() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 10,
            max_tool_concurrency: 4,
        });
        runtime.commit_file_write(7);
        assert!(runtime.check_file_write(2).is_ok());
        let err = runtime.check_file_write(4).unwrap_err();
        assert_eq!(err.requested, 11);
    }

    #[test]
    fn test_file_write_reservation_prevents_concurrent_overspend() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 10,
            max_tool_concurrency: 4,
        });
        assert_eq!(runtime.try_reserve_file_write("a", 8).unwrap(), "a");
        let err = runtime.try_reserve_file_write("b", 4).unwrap_err();
        assert_eq!(err.resource, ResourceKind::FileWrite);
        assert!(runtime.commit_file_write_reservation("a"));
        assert_eq!(runtime.usage().file_write_bytes, 8);
    }

    #[test]
    fn test_tool_concurrency_acquire_and_release() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 1024,
            max_tool_concurrency: 1,
        });
        let token = runtime.try_acquire_tool("tool-1").unwrap();
        assert_eq!(runtime.usage().active_tools, 1);
        let err = runtime.try_acquire_tool("tool-2").unwrap_err();
        assert_eq!(err.resource, ResourceKind::Tool);
        assert!(runtime.release_tool(&token));
        assert_eq!(runtime.usage().active_tools, 0);
    }

    #[test]
    fn test_worker_concurrency_acquire_and_release() {
        let mut runtime = RuntimeManager::with_budget(RuntimeBudget {
            max_sidecars: 1,
            max_workers: 1,
            max_memory_mb: 128,
            max_tokens: 100,
            max_file_write_bytes: 1024,
            max_tool_concurrency: 4,
        });
        let token = runtime.try_acquire_worker("agent-1").unwrap();
        assert_eq!(runtime.usage().active_workers, 1);
        assert_eq!(runtime.try_acquire_worker("agent-1").unwrap(), "agent-1");
        assert_eq!(runtime.usage().active_workers, 1);
        let err = runtime.try_acquire_worker("agent-2").unwrap_err();
        assert_eq!(err.resource, ResourceKind::Worker);
        assert!(runtime.release_worker(&token));
        assert_eq!(runtime.usage().active_workers, 0);
        assert!(runtime.try_acquire_worker("agent-2").is_ok());
    }
}
