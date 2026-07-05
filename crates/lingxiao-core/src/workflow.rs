#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowExecutionStatus {
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowExecutionStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn can_transition_to(self, to: Self) -> bool {
        use WorkflowExecutionStatus::*;
        matches!(
            (self, to),
            (Running, Completed | Failed | Paused | Cancelled)
                | (Paused, Running | Failed | Cancelled)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowPlanError {
    DuplicateNode(String),
    MissingNodeId,
    UnknownEdgeEndpoint(String),
    CycleDetected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowNodePlan {
    pub id: String,
}

pub fn plan_dag_execution(
    nodes: &[serde_json::Value],
    edges: &[serde_json::Value],
) -> Result<Vec<WorkflowNodePlan>, WorkflowPlanError> {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    let mut node_order = Vec::new();
    let mut outgoing: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut indegree: BTreeMap<String, usize> = BTreeMap::new();

    for node in nodes {
        let Some(node_id) = node
            .get("id")
            .or_else(|| node.get("node_id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
        else {
            return Err(WorkflowPlanError::MissingNodeId);
        };
        if indegree.contains_key(&node_id) {
            return Err(WorkflowPlanError::DuplicateNode(node_id));
        }
        node_order.push(node_id.clone());
        indegree.insert(node_id.clone(), 0);
        outgoing.insert(node_id, BTreeSet::new());
    }

    for edge in edges {
        let from = edge_endpoint(edge, &["from", "source", "source_id"])?;
        let to = edge_endpoint(edge, &["to", "target", "target_id"])?;
        if !indegree.contains_key(&from) {
            return Err(WorkflowPlanError::UnknownEdgeEndpoint(from));
        }
        if !indegree.contains_key(&to) {
            return Err(WorkflowPlanError::UnknownEdgeEndpoint(to));
        }
        if outgoing.entry(from).or_default().insert(to.clone()) {
            *indegree.get_mut(&to).expect("checked above") += 1;
        }
    }

    let mut ready: VecDeque<String> = node_order
        .iter()
        .filter(|id| indegree.get(*id).copied().unwrap_or(0) == 0)
        .cloned()
        .collect();
    let mut planned = Vec::new();

    while let Some(node_id) = ready.pop_front() {
        planned.push(WorkflowNodePlan {
            id: node_id.clone(),
        });
        if let Some(children) = outgoing.get(&node_id) {
            for child in children {
                let child_indegree = indegree.get_mut(child).expect("child exists");
                *child_indegree -= 1;
                if *child_indegree == 0 {
                    ready.push_back(child.clone());
                }
            }
        }
    }

    if planned.len() != node_order.len() {
        return Err(WorkflowPlanError::CycleDetected);
    }
    Ok(planned)
}

fn edge_endpoint(edge: &serde_json::Value, keys: &[&str]) -> Result<String, WorkflowPlanError> {
    keys.iter()
        .find_map(|key| edge.get(*key).and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or(WorkflowPlanError::MissingNodeId)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_transitions() {
        assert!(WorkflowExecutionStatus::Running.can_transition_to(WorkflowExecutionStatus::Paused));
        assert!(WorkflowExecutionStatus::Paused.can_transition_to(WorkflowExecutionStatus::Running));
        assert!(
            !WorkflowExecutionStatus::Completed.can_transition_to(WorkflowExecutionStatus::Running)
        );
    }

    #[test]
    fn test_p5_plan_dag_execution_topological_order() {
        let nodes = vec![
            serde_json::json!({"id": "B"}),
            serde_json::json!({"id": "A"}),
            serde_json::json!({"id": "C"}),
        ];
        let edges = vec![
            serde_json::json!({"from": "A", "to": "B"}),
            serde_json::json!({"from": "B", "to": "C"}),
        ];

        let plan = plan_dag_execution(&nodes, &edges).unwrap();
        let ids: Vec<_> = plan.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(ids, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_p5_plan_dag_execution_rejects_cycles() {
        let nodes = vec![
            serde_json::json!({"id": "A"}),
            serde_json::json!({"id": "B"}),
        ];
        let edges = vec![
            serde_json::json!({"from": "A", "to": "B"}),
            serde_json::json!({"from": "B", "to": "A"}),
        ];

        assert_eq!(
            plan_dag_execution(&nodes, &edges),
            Err(WorkflowPlanError::CycleDetected)
        );
    }
}
