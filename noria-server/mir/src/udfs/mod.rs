
use crate::node::{MirNode, MirNodeType };
use column::Column;
use query::{MirQuery};
use MirNodeRef;

// <begin(graph-mods)>
mod composition_graph;
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
    pub sources: Vec<(u32, usize, Vec<Column> )>,

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
        "composition" => Some(composition_graph::mk_graph()),
        // <end(graph-dispatch)>
        _ => None
    }
}

pub fn as_mir_query(name: String, tables: &[String], bases: Vec<MirNodeRef>) -> Result<MirQuery, String> {
    let gr = get_graph(&name, tables).ok_or(format!("No UDTF named {} found", name))?;

    let roots : Vec<MirNodeRef> = bases;
    let schema_version = 0;

    let output : Vec<Column> = Vec::new();
    let successors : Vec<MirNodeRef> = Vec::new();

    let no_node = MirNode::new(
        "drop me please",
        schema_version,
        vec![],
        MirNodeType::Identity,
        vec![],
        vec![],
    );

    let key_col_name = format!("{}-gen-key", name);
    let key_col = Column::new(None, &key_col_name);

    let bottom = {
        let (parent, ref non_key_cols) = gr.sink;
        let mut cols = non_key_cols.clone();
        cols.push(key_col.clone());
        MirNode::new(
            format!("{}-exit", &name).as_ref(),
            schema_version,
            cols,
            MirNodeType::Project {
                emit: non_key_cols.clone(),
                literals: vec![(key_col_name, 0.into())],
                arithmetic: vec![],
            },
            vec![],
            successors.clone(),
        )
    };

    let mut a_list = gr.adjacency_list;
    let (new_nodes, adjacencies) = {
        let num_nodes =
            a_list.len() // new nodes
            + roots.len() // input relations
            + 1; // the bottom/output

        let mut new_nodes = Vec::with_capacity(num_nodes);
        let mut adjacencies: Vec<Vec<usize>> = Vec::with_capacity(num_nodes);
        new_nodes.push(bottom.clone());
        new_nodes.extend(roots.clone());
        assert!(gr.sources.len() == roots.len());
        adjacencies.push(vec![gr.sink.0]); // Sink node. (sole) parent is recorded in graph
        adjacencies.extend(a_list.drain(..).enumerate().map(
            |(i, (inner, cols, adj))| {
                let name = match inner {
                    MirNodeType::UDFBasic {
                        ref function_name, ..
                    } => function_name.clone(),
                    _ => format!("{}-n{}", &name, i),
                };
                new_nodes.push(MirNode::new(
                    name.as_ref(),
                    schema_version,
                    cols,
                    inner,
                    vec![],
                    vec![],
                ));
                adj
            },
        ));

        (new_nodes, adjacencies)
    };
    //eprintln!("{:?}", &adjacencies);
    let link = |n_ref: MirNodeRef, adj: &Vec<usize>| {
        let mut n = n_ref.borrow_mut();
        for other_idx in adj {
            let other_ref = new_nodes[*other_idx].clone();
            let mut other = other_ref.borrow_mut();
            other.children.push(n_ref.clone());
            n.ancestors.push(other_ref.clone());
        }
    };
    for (self_idx, adj) in adjacencies.iter().enumerate() {
        let n_ref = new_nodes[self_idx].clone();
        link(n_ref, adj);
    }

    if let MirNodeType::Leaf{node: ref mut n,..} = bottom.borrow_mut().inner {
            std::mem::replace(n, new_nodes[gr.sink.0].clone());
    } else {
        panic!("impossible");
    }

    let leaf = {
        let (parent, cols) = gr.sink;
        MirNode::new(
            &name,
            schema_version,
            output.clone(),
            MirNodeType::Leaf {
                node: bottom.clone(),
                keys: vec![key_col.clone()],
            },
            vec![bottom],
            vec![],
        )
    };

    Ok(MirQuery {
        name,
        roots,
        leaf,
    })
}
