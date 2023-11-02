use super::SparqlQueryable;
use async_trait::async_trait;
use oxigraph::sparql::QueryResults;
use oxigraph::store::Store;
use sparesults::QuerySolution;
use spargebra::Query;
use std::error::Error;

pub struct EmbeddedOxigraph {
    pub store: Store,
}

#[async_trait]
impl SparqlQueryable for EmbeddedOxigraph {
    async fn execute(&mut self, query: &Query) -> Result<Vec<QuerySolution>, Box<dyn Error>> {
        let oxiquery = oxigraph::sparql::Query::parse(query.to_string().as_str(), None).unwrap();
        let res = self.store.query(oxiquery).map_err(|x| Box::new(x))?;
        match res {
            QueryResults::Solutions(sols) => {
                let mut output = vec![];
                for s in sols {
                    output.push(s?);
                }
                Ok(output)
            }
            _ => panic!("Should never happen"),
        }
    }
}
