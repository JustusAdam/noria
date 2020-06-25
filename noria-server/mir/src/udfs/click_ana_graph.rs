use crate::node::MirNodeType;
use column::Column;
use super::{ExecutionType, UDFGraph};

pub fn mk_graph() -> UDFGraph {
    // <begin(graph)>
    UDFGraph{adjacency_list: vec![ ( MirNodeType::UDFBasic {function_name: "ohua.generated/op_s_sequences_0_0".to_string()
    ,indices: vec![Column::new(Option::None, "o-1_i1"), Column::new(Option::None, "o-1_i2")]
    ,execution_type: ExecutionType::Reduction{group_by: vec![Column::new(Option::None, "o-1_i0")]}}
    , vec![Column::new(Option::None, "o-1_i0"), Column::new(Option::None, "o8_i0")]
    , vec![0] ) ]
    ,sink: (2, vec![Column::new(Option::None, "o-1_i0"), Column::new(Option::None, "o8_i0")])
    ,source: vec![ Column::new(Option::None, "o-1_i0")
    , Column::new(Option::None, "o-1_i1")
    , Column::new(Option::None, "o-1_i2") ]}
    // <end(graph)>
}
