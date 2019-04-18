
1. Add a mod for the new op under `dataflow/src/ops`
    - Don't forget to `#[derive(Debug, Clone, Serialize, Deserialize)]` for the
      new data structure
1. Add the op in the `NodeOperator` enum
1. Add `nodeop_from_impl!` invocation
1. Add `impl_ingredient_fn_mut` and `impl_ingredient_fn_ref` cases
