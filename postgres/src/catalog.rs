use representation::BaseRDFNodeType;
use spargebra::{ParseError, Query};
use std::collections::HashMap;

pub struct Catalog {
    pub data_products: HashMap<String, DataProduct>,
}
pub struct DataProduct {
    pub query_string: String,
    pub parsed_query: Option<Query>,
    pub rdf_node_types: HashMap<String, BaseRDFNodeType>,
}

impl DataProduct {
    pub fn init(&mut self) -> Result<(), ParseError> {
        self.parsed_query = Some(Query::parse(&self.query_string, None)?);
        Ok(())
    }
}
