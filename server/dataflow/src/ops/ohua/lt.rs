use std::collections::HashMap;

use crate::prelude::*;

/// Applies the identity operation to the view. Since the identity does nothing,
/// it is the simplest possible operation. Primary intended as a reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lt {
    src: IndexPair,
    l: usize,
    r: usize,
    carry: usize,
}

impl Lt {
    /// Construct a new identity operator.
    pub fn new(src: NodeIndex, l: usize, r: usize, carry: usize) -> Lt {
        Lt { src: src.into(), l, r , carry}
    }
}

impl Ingredient for Lt {
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
        mut rs: Records,
        _: Option<&[usize]>,
        _: &DomainNodes,
        _: &StateMap,
    ) -> ProcessingResult {
        for ref mut r in rs.iter_mut() {
            let res = r[self.l] < r[self.r];
            r.push(DataType::Int(if res { 1 } else { 0 }));
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
        "lt".into()
    }

    fn parent_columns(&self, column: usize) -> Vec<(NodeIndex, Option<usize>)> {
        if column == self.carry {
            vec![(self.src.as_global(), Some(self.l)), (self.src.as_global(), Some( self.r ))]
        } else {
            vec![(self.src.as_global(), Some(column))]
        }
    }
}
