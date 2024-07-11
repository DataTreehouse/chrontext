pub mod errors;
pub mod python;

pub mod bigquery;
pub mod opcua;

use crate::bigquery::VirtualizedBigQueryDatabase;
use crate::errors::VirtualizedDatabaseError;
use crate::opcua::VirtualizedOPCUADatabase;
use crate::python::VirtualizedPythonDatabase;
use oxrdf::NamedNode;
use polars::prelude::{DataFrame, DataType};
use representation::polars_to_rdf::polars_type_to_literal_type;
use representation::solution_mapping::EagerSolutionMappings;
use representation::RDFNodeType;
use std::collections::{HashMap, HashSet};
use templates::ast::{ConstantTerm, ConstantTermOrList, StottrTerm, Template};
use templates::constants::OTTR_TRIPLE;
use virtualized_query::pushdown_setting::PushdownSetting;
use virtualized_query::{VirtualizedQuery, ID_VARIABLE_NAME};

#[derive(Debug)]
pub struct Virtualization {
    pub resources: HashMap<String, Template>,
}

impl Virtualization {
    pub fn get_virtualized_iris(&self) -> HashSet<NamedNode> {
        let mut nns = HashSet::new();
        for t in self.resources.values() {
            for i in &t.pattern_list {
                assert_eq!(i.template_name.as_str(), OTTR_TRIPLE);
                let a = i.argument_list.get(1).unwrap();
                if let StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                    ConstantTerm::Iri(nn),
                )) = &a.term
                {
                    nns.insert(nn.clone());
                } else {
                    todo!("Handle this error")
                }
            }
        }
        nns
    }
    pub fn get_first_level_virtualized_iris(&self) -> HashSet<NamedNode> {
        let mut nns = HashSet::new();
        for t in self.resources.values() {
            for i in &t.pattern_list {
                assert_eq!(i.template_name.as_str(), OTTR_TRIPLE);
                let subj = i.argument_list.get(0).unwrap();
                if let StottrTerm::Variable(v) = &subj.term {
                    if v.as_str() == ID_VARIABLE_NAME {
                        let a = i.argument_list.get(1).unwrap();
                        if let StottrTerm::ConstantTerm(ConstantTermOrList::ConstantTerm(
                            ConstantTerm::Iri(nn),
                        )) = &a.term
                        {
                            nns.insert(nn.clone());
                        } else {
                            todo!("Handle this error")
                        }
                    }
                }
            }
        }
        nns
    }
}

pub enum VirtualizedDatabase {
    VirtualizedPythonDatabase(VirtualizedPythonDatabase),
    VirtualizedBigQueryDatabase(VirtualizedBigQueryDatabase),
    VirtualizedOPCUADatabase(VirtualizedOPCUADatabase),
}

impl VirtualizedDatabase {
    pub fn pushdown_settings(&self) -> HashSet<PushdownSetting> {
        match self {
            VirtualizedDatabase::VirtualizedPythonDatabase(pydb) => pydb.pushdown_settings(),
            VirtualizedDatabase::VirtualizedBigQueryDatabase(_) => {
                VirtualizedBigQueryDatabase::pushdown_settings()
            }
            VirtualizedDatabase::VirtualizedOPCUADatabase(_) => {
                VirtualizedOPCUADatabase::pushdown_settings()
            }
        }
    }

    pub async fn query(
        &self,
        vq: &VirtualizedQuery,
    ) -> Result<EagerSolutionMappings, VirtualizedDatabaseError> {
        match self {
            VirtualizedDatabase::VirtualizedPythonDatabase(pyvdb) => {
                let df = pyvdb.query(vq).map_err(VirtualizedDatabaseError::from)?;
                let rdf_node_types = get_datatype_map(&df);
                Ok(EagerSolutionMappings::new(df, rdf_node_types))
            }
            VirtualizedDatabase::VirtualizedBigQueryDatabase(q) => q.query(vq).await,
            VirtualizedDatabase::VirtualizedOPCUADatabase(uadb) => uadb.query(vq).await,
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
