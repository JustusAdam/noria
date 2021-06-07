use std::collections::HashMap;

use crate::prelude::*;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    src: IndexPair,
    tag: usize,
    batch: usize,
}

impl Trace {
    /// Construct a new identity operator.
    pub fn new(tag: usize, src: NodeIndex) -> Self {
        Self { src: src.into(), tag, batch: 0 }
    }
}

impl Ingredient for Trace {
    fn take(&mut self) -> NodeOperator {
        Clone::clone(self).into()
    }

    fn ancestors(&self) -> Vec<NodeIndex> {
        vec![self.src.as_global()]
    }

    fn on_connected(&mut self, _: &Graph) {}

    fn on_commit(&mut self, _: NodeIndex, remap: &HashMap<NodeIndex, IndexPair>) {
        self.src.remap(remap);
    }

    fn on_input(
        &mut self,
        _: &mut dyn Executor,
        _: LocalNodeIndex,
        rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        eprintln!("Tracer {} batch {}:", self.tag, self.batch);
        self.batch += 1;
        for r in rs.iter() {
            eprintln!("   {:?}", r);
        }
        ProcessingResult {
            results: rs,
            ..Default::default()
        }
    }

    fn suggest_indexes(&self, _: NodeIndex) -> HashMap<NodeIndex, Vec<usize>> {
        HashMap::new()
    }

    fn resolve(&self, col: usize) -> Option<Vec<(NodeIndex, usize)>> {
        Some(vec![(self.src.as_global(), col)])
    }

    fn description(&self, _: bool) -> String {
        "≡".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        vec![(self.src.as_global(), Some(column))]
    }
}
