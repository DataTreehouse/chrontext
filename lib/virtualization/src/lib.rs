pub mod errors;
pub mod python;

use crate::errors::VirtualizedDatabaseError;
use crate::python::PyVirtualizedDatabase;
use polars::prelude::{DataFrame, DataType};
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::VirtualizedQuery;

#[derive(Debug)]
pub enum VirtualizedDatabase {
    PyVirtualizedDatabase(PyVirtualizedDatabase),
}

impl VirtualizedDatabase {
    pub fn pushdown_settings(&self) -> HashSet<PushdownSetting> {
        match self {
            VirtualizedDatabase::PyVirtualizedDatabase(_) => all_pushdowns(),
        }
    }

    pub async fn query(
        &self,
        vq: &VirtualizedQuery,
    ) -> Result<EagerSolutionMappings, VirtualizedDatabaseError> {
        match self {
            VirtualizedDatabase::PyVirtualizedDatabase(pyvdb) => {
                let df = pyvdb.query(vq).map_err(VirtualizedDatabaseError::from)?;
                let rdf_node_types = get_datatype_map(&df);
                Ok(EagerSolutionMappings::new(df, rdf_node_types))
            }
        }
    }
}

pub fn get_datatype_map(df: &DataFrame) -> HashMap<String, RDFNodeType> {
    let mut map = HashMap::new();
    for c in df.columns(df.get_column_names()).unwrap() {
        let dtype = c.dtype();
        if let &DataType::Null = dtype {
            map.insert(c.name().to_string(), RDFNodeType::None);
        } else {
            map.insert(
                c.name().to_string(),
                polars_type_to_literal_type(dtype).unwrap().to_owned(),
            );
        }
    }
    map
}
