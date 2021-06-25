use crate::node::{MirNode, MirNodeType};
use column::Column;
use query::MirQuery;
use std::collections::HashMap;
use MirNodeRef;

// <begin(graph-mods)>
mod main_graph;
// <end(graph-mods)>

pub type Columns = Vec<Column>;
/// IMPORTANT! The indices in the adjacency list are *not correct*. They are all
/// offset by 2. The idea is that when constructing the graph the first element
/// of the `MirNode` vector is the parent node and the last one is the final
/// project. All other nodes are follow, in the same order as in this vector. In
/// this setup the indices then directly correspond to the indices in the
/// resulting vector.
pub struct UDFGraph {
    pub adjacency_list: Vec<(MirNodeType, Columns, Vec<usize>)>,
    pub sink: (usize, Columns, Columns),
    pub sources: (Vec<Columns>, Vec<(String, Columns)>),
}

pub enum ExecutionType {
    Reduction { group_by: Vec<Column> },
    Simple { carry: usize },
}

pub struct UDTFIncorporator {
    log: slog::Logger,
    pub schema_version: usize,
}

impl UDTFIncorporator {
    pub fn new(log: slog::Logger) -> UDTFIncorporator {
        UDTFIncorporator { log, schema_version: 0 }
    }

    pub fn is_defined(&self, name: &str) -> bool {
        self.get_graph(name).is_some()
    }

    fn get_graph(&self, gr: &str) -> Option<Box<dyn Fn(&[String]) -> UDFGraph>> {
        match gr.as_ref() {
            // <begin(graph-dispatch)>
            "main" => Some(Box::new(main_graph::mk_graph)),
            // <end(graph-dispatch)>
            _ => None,
        }
    }

    pub fn as_mir_query<F: Fn(&str) -> MirNodeRef>(
        &self,
        name: &str,
        tables: &[String],
        bases: Vec<MirNodeRef>,
        resolve_named: F,
    ) -> Result<MirQuery, String> {
        let mut new_tables = {
            let mut seen = HashMap::new();
            tables
                .iter()
                .map(|t| {
                    if let Some(n) = seen.get_mut(t) {
                        *n += 1;
                        format!("{}({})", t, *n)
                    } else {
                        seen.insert(t, 0);
                        t.clone()
                    }
                })
                .collect::<Vec<_>>()
        };

        let gr = self
            .get_graph(&name)
            .ok_or(format!("No UDTF named '{}' found", name))?(&new_tables);

        let mut roots: Vec<MirNodeRef> = bases;
        let schema_version = self.schema_version;

        let key_col_name = format!("{}-gen-key", name);
        let key_col = Column::new(None, &key_col_name);
        let mut output = gr.sink.2.clone();
        output.push(key_col.clone());

        let bottom = {
            MirNode::new(
                format!("{}-exit", &name).as_ref(),
                schema_version,
                output.clone(),
                MirNodeType::Project {
                    emit: gr.sink.1.clone(),
                    literals: vec![(key_col_name, 0.into())],
                    arithmetic: vec![],
                },
                vec![],
                vec![],
            )
        };

        let mut a_list = gr.adjacency_list;
        let mut named_sources: Vec<MirNodeRef> =
            gr.sources.1.iter().map(|(n, _)| resolve_named(n)).collect();
        let num_nodes = a_list.len() // new nodes
            + roots.len() // input relation projections
            + gr.sources.1.len() // named input relation projections
            + 1; // the bottom/output projection
        let trace_these: Vec<_> = std::env::var("NORIA_TRACE_NODES")
            .map(|s| {
                s.split(",")
                    .map(|s| s.parse::<usize>().unwrap())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|_| vec![]);
        let adjacencies = {
            assert!(gr.sources.0.len() == roots.len());
            let mut adjacencies: Vec<(MirNodeRef, Vec<usize>)> = Vec::with_capacity(num_nodes);
            adjacencies.push((bottom.clone(), vec![gr.sink.0]));
            adjacencies.extend(
                roots
                    .iter()
                    .zip(gr.sources.0.iter())
                    .zip(new_tables.iter().zip(tables.iter()))
                    .map(|((r, cols), (new, old))| {
                        // Adds a project for each base table
                        let emit_cols = {
                            let mut new_cols = cols.clone();
                            if old != new {
                                println!("Replacing {} with {}", new, old);
                                new_cols.iter_mut().for_each(|c| {
                                    assert_eq!(c.table.as_ref(), Some(new));
                                    c.table.replace(old.clone());
                                })
                            }
                            new_cols
                        };
                        let n = MirNode::new(
                            format!("project-{}-for-{}", new, &name).as_ref(),
                            schema_version,
                            cols.clone(),
                            MirNodeType::Project {
                                emit: emit_cols,
                                literals: vec![],
                                arithmetic: vec![],
                            },
                            vec![r.clone()],
                            vec![],
                        );
                        (n, vec![])
                    }),
            );
            adjacencies.extend(gr.sources.1.iter().zip(named_sources.iter()).map(
                |((name, cols), n)| {
                    // Adds a project for each base table
                    let n = MirNode::new(
                        format!("project-{}", &name).as_ref(),
                        schema_version,
                        cols.clone(),
                        MirNodeType::Project {
                            emit: cols.clone(),
                            literals: vec![],
                            arithmetic: vec![],
                        },
                        vec![n.clone()],
                        vec![],
                    );
                    (n, vec![])
                },
            ));
            adjacencies.extend(a_list.drain(..).enumerate().map(|(i, (inner, cols, adj))| {
                let name = match inner {
                    MirNodeType::UDFBasic {
                        ref function_name, ..
                    } => format!("{}-n{}", function_name, i),
                    _ => format!("{}-n{}", &name, i),
                };
                let n = MirNode::new(name.as_ref(), schema_version, cols, inner, vec![], vec![]);
                (n, adj)
            }));
            adjacencies
        };
        assert_eq!(adjacencies.len(), num_nodes);
        //eprintln!("{:?}", &adjacencies);
        let link = |n_ref: MirNodeRef, adj: &Vec<usize>| {
            let mut n = n_ref.borrow_mut();
            debug!(self.log, "Linking {} to {:?}", n.name, adj);
            for other_idx in adj {
                let other_ref = adjacencies[*other_idx].0.clone();
                if std::rc::Rc::ptr_eq(&n_ref, &other_ref) {
                    panic!("Trying to link ref for {} with itself", n.name);
                }
                let mut other = other_ref.borrow_mut();
                other.add_child(n_ref.clone());
                n.add_ancestor(other_ref.clone());
            }
        };
        for (n_ref, adj) in adjacencies.iter() {
            link(n_ref.clone(), adj);
        }
        for idx in trace_these {
            println!("Tracing {}", idx);
            let n = &adjacencies[idx].0;
            let cols = n.borrow().columns.clone();
            let mut outs = vec![];
            std::mem::swap(&mut n.borrow_mut().children, &mut outs);
            let new = MirNode::new(
                &format!("Tracer-{}", idx),
                schema_version,
                cols,
                MirNodeType::Trace { tag: idx },
                vec![n.clone()],
                outs
            );
            for c in new.borrow().children() {
                for a in c.borrow_mut().ancestors.iter_mut() {
                    if std::rc::Rc::ptr_eq(a, n) {
                        *a = new.clone()
                    }
                }
            }
        }

        let leaf = {
            MirNode::new(
                &name,
                schema_version,
                output,
                MirNodeType::Leaf {
                    node: bottom.clone(),
                    keys: vec![key_col.clone()],
                },
                vec![bottom.clone()],
                vec![],
            )
        };
        roots.extend(named_sources.drain(..));
        for root in roots.iter() {
            assert_ne!(root.borrow().children().len(), 0);
        }
        let name = name.to_string();
        let q = MirQuery { name, roots, leaf };

        debug!(self.log, "Created mir query {}", &q);

        Ok(q)
    }
}
