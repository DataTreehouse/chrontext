// Uses code from https://github.com/magbak/maplib/blob/main/triplestore/src/sparql/solution_mapping.rs

use oxrdf::vocab::xsd;
use oxrdf::{NamedNode};
use polars::prelude::LazyFrame;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct SolutionMappings {
    pub mappings: LazyFrame,
    pub columns: HashSet<String>,
    pub datatypes: HashMap<String, NamedNode>,
}

impl SolutionMappings {
    pub fn new(
        mappings: LazyFrame,
        columns: HashSet<String>,
        datatypes: HashMap<String, NamedNode>,
    ) -> SolutionMappings {
        SolutionMappings {
            mappings,
            columns,
            datatypes,
        }
    }
}

pub fn is_string_col(node: &NamedNode) -> bool {
    if node.as_ref() == xsd::STRING {
        return true;
    }
    return false;
}
