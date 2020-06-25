
use crate::node::MirNodeType;
use column::Column;

// <begin(graph-mods)>
mod click_ana_graph;
// <end(graph-mods)>


/// IMPORTANT! The indices in the adjacency list are *not correct*. They are all
/// offset by 2. The idea is that when constructing the graph the first element
/// of the `MirNode` vector is the parent node and the last one is the final
/// project. All other nodes are follow, in the same order as in this vector. In
/// this setup the indices then directly correspond to the indices in the
/// resulting vector.
pub struct UDFGraph {
    pub adjacency_list: Vec<(MirNodeType, Vec<Column>, Vec<usize>)>,
    pub sink: (usize, Vec<Column>),
    pub source: Vec<Column>,

}

pub enum ExecutionType {
    Reduction {
        group_by: Vec<Column>,
    },
    Simple {
        carry: usize
    },
}

pub fn get_graph(gr: &String) -> Option<UDFGraph> {
    match gr.as_ref() {
        // <begin(graph-dispatch)>
        "click_ana" => Some(click_ana_graph::mk_graph()),
        // <end(graph-dispatch)>
        _ => None
    }
}
