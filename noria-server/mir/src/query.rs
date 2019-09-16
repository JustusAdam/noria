use std::collections::HashMap;
use std::fmt::{Display, Error, Formatter};

use petgraph::graph::NodeIndex;
use MirNodeRef;

/// Represents the result of a query incorporation, specifying query name (auto-generated or
/// reflecting a pre-specified name), new nodes added for the query, reused nodes that are part of
/// the query, and the leaf node that represents the query result (and off whom we've hung a
/// `Reader` node),
#[derive(Clone, Debug, PartialEq)]
pub struct QueryFlowParts {
    pub name: String,
    pub new_nodes: Vec<NodeIndex>,
    pub reused_nodes: Vec<NodeIndex>,
    pub query_leaf: NodeIndex,
}

pub struct QueryIter {
    current: Vec<MirNodeRef>,
    next: Vec<MirNodeRef>,
}

impl QueryIter {
    // chosen by fair dice roll
    // guaranteed to be random
    const EXTRA_CAP: usize = 8;

    pub fn new(roots: &Vec<MirNodeRef>) -> Self {
        QueryIter {
            current: roots.clone(),
            next: Vec::with_capacity(roots.len() + Self::EXTRA_CAP),
        }
    }
}

impl Iterator for QueryIter {
    type Item = MirNodeRef;

    fn next(&mut self) -> Option<Self::Item> {
        // There should be a better way to do this but the only one I can think
        // ow immediately is this. The reason I do this is to avoid traversing
        // the same node twice which may lead to inconsistencies.
        use std::rc::Rc;
        if let Some(mref) = self.current.pop() {
            let curr = &self.current;
            self.next.extend(
                mref.borrow()
                    .children()
                    .iter()
                    .filter(|c| !curr.iter().any(|c2| Rc::ptr_eq(c, c2)))
                    .cloned(),
            );
            Some(mref)
        } else {
            if self.next.is_empty() {
                None
            } else {
                while let Some(n) = self.next.pop() {
                    if !self.next.iter().any(|c| Rc::ptr_eq(&n, c)) {
                        self.current.push(n);
                    }
                }
                self.next()
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MirQuery {
    pub name: String,
    pub roots: Vec<MirNodeRef>,
    pub leaf: MirNodeRef,
}

impl MirQuery {
    pub fn singleton(name: &str, node: MirNodeRef) -> MirQuery {
        MirQuery {
            name: String::from(name),
            roots: vec![node.clone()],
            leaf: node,
        }
    }

    pub fn iter(&self) -> QueryIter {
        QueryIter::new(&self.roots)
    }

    #[cfg(test)]
    pub fn topo_nodes(&self) -> Vec<MirNodeRef> {
        use std::collections::VecDeque;

        let mut nodes = Vec::new();

        // starting at the roots, traverse in topological order
        let mut node_queue: VecDeque<_> = self.roots.iter().cloned().collect();
        let mut in_edge_counts = HashMap::new();
        for n in &node_queue {
            in_edge_counts.insert(n.borrow().versioned_name(), 0);
        }
        while let Some(n) = node_queue.pop_front() {
            assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

            nodes.push(n.clone());

            for child in n.borrow().children.iter() {
                let nd = child.borrow().versioned_name();
                let in_edges = if in_edge_counts.contains_key(&nd) {
                    in_edge_counts[&nd]
                } else {
                    child.borrow().ancestors.len()
                };
                assert!(in_edges >= 1, format!("{} has no incoming edges!", nd));
                if in_edges == 1 {
                    // last edge removed
                    node_queue.push_back(child.clone());
                }
                in_edge_counts.insert(nd, in_edges - 1);
            }
        }
        nodes
    }

    pub fn optimize(
        mut self,
        table_mapping: Option<&HashMap<(String, Option<String>), String>>,
        sec: bool,
    ) -> MirQuery {
        super::rewrite::pull_required_base_columns(&mut self, table_mapping, sec);
        super::optimize::optimize(self)
    }

    pub fn optimize_post_reuse(mut self) -> MirQuery {
        super::optimize::optimize_post_reuse(&mut self);
        self
    }

    pub fn make_universe_naming_consistent(
        mut self,
        table_mapping: &HashMap<(String, Option<String>), String>,
        base_name: String,
    ) -> MirQuery {
        super::rewrite::make_universe_naming_consistent(&mut self, table_mapping, base_name);
        self
    }
}

impl Display for MirQuery {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        use std::collections::VecDeque;

        // starting at the roots, print nodes in topological order
        let mut node_queue = VecDeque::new();
        node_queue.extend(self.roots.iter().cloned());
        let mut in_edge_counts = HashMap::new();
        for n in &node_queue {
            in_edge_counts.insert(n.borrow().versioned_name(), 0);
        }

        while !node_queue.is_empty() {
            let n = node_queue.pop_front().unwrap();
            assert_eq!(in_edge_counts[&n.borrow().versioned_name()], 0);

            writeln!(f, "{} MIR node {:?}", self.name, n.borrow())?;

            for child in n.borrow().children.iter() {
                let nd = child.borrow().versioned_name();
                let in_edges = if in_edge_counts.contains_key(&nd) {
                    in_edge_counts[&nd]
                } else {
                    child.borrow().ancestors.len()
                };
                assert!(in_edges >= 1);
                if in_edges == 1 {
                    // last edge removed
                    node_queue.push_back(child.clone());
                }
                in_edge_counts.insert(nd, in_edges - 1);
            }
        }

        Ok(())
    }
}
