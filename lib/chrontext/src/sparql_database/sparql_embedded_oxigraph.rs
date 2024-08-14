use super::{SparqlQueryError, SparqlQueryable};
use async_trait::async_trait;
use filesize::PathExt;
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::sparql::QueryResults;
use oxigraph::store::Store;
use sparesults::QuerySolution;
use spargebra::Query;
use std::fs::{read_to_string, File};
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;
use thiserror::Error;

const RDF_FILE_METADATA: &str = "rdf_file_data.txt";

#[derive(Debug, Error)]
pub enum EmbeddedOxigraphError {
    #[error("Error opening oxigraph storage path `{0}`")]
    OpenStorageError(String),
    #[error("Error reading NTriples file `{0}`")]
    ReadNTriplesFileError(String),
    #[error("Error reading loading NTriples file `{0}`")]
    LoaderError(String),
    #[error("Oxigraph metadata IO Error `{0}`")]
    DBMetadataIOError(String),
    #[error("Oxigraph evaluation error")]
    EvaluationError(String),
}

#[derive(Debug)]
pub struct EmbeddedOxigraphConfig {
    pub path: Option<String>,
    pub rdf_file: String,
    pub rdf_format: Option<RdfFormat>,
}

pub struct EmbeddedOxigraph {
    pub store: Store,
}

#[async_trait]
impl SparqlQueryable for EmbeddedOxigraph {
    async fn execute(&self, query: &Query) -> Result<Vec<QuerySolution>, SparqlQueryError> {
        let oxiquery = oxigraph::sparql::Query::parse(query.to_string().as_str(), None).unwrap();
        let res = self.store.query(oxiquery).map_err(|x| {
            SparqlQueryError::EmbeddedOxigraphError(EmbeddedOxigraphError::EvaluationError(
                x.to_string(),
            ))
        })?;
        match res {
            QueryResults::Solutions(sols) => {
                let mut output = vec![];
                for s in sols {
                    output.push(s.map_err(|x| {
                        SparqlQueryError::EmbeddedOxigraphError(
                            EmbeddedOxigraphError::EvaluationError(x.to_string()),
                        )
                    })?);
                }
                Ok(output)
            }
            _ => panic!("Should never happen"),
        }
    }
}

impl EmbeddedOxigraph {
    pub fn from_config(
        config: EmbeddedOxigraphConfig,
    ) -> Result<EmbeddedOxigraph, EmbeddedOxigraphError> {
        let path = Path::new(&config.rdf_file);

        let rdf_format = if let Some(rdf_format) = &config.rdf_format {
            rdf_format.clone()
        } else {
            if path.extension() == Some("ttl".as_ref()) {
                RdfFormat::Turtle
            } else if path.extension() == Some("nt".as_ref()) {
                RdfFormat::NTriples
            } else if path.extension() == Some("xml".as_ref()) {
                RdfFormat::RdfXml
            } else {
                todo!("Have not implemented file format {:?}", path);
            }
        };

        let rdf_file_metadata = file_metadata_string(path)
            .map_err(|x| EmbeddedOxigraphError::DBMetadataIOError(x.to_string()))?;

        let store = if let Some(p) = &config.path {
            Store::open(Path::new(p))
        } else {
            Store::new()
        }
        .map_err(|x| EmbeddedOxigraphError::OpenStorageError(x.to_string()))?;

        let need_read_file = if let Some(p) = &config.path {
            let mut pb = Path::new(p).to_path_buf();
            pb.push(Path::new(RDF_FILE_METADATA));
            let dbdata_path = pb.as_path();
            if dbdata_path.exists() {
                let existing_db_rdf_metadata = read_to_string(dbdata_path)
                    .map_err(|x| EmbeddedOxigraphError::DBMetadataIOError(x.to_string()))?;
                existing_db_rdf_metadata != rdf_file_metadata
            } else {
                true
            }
        } else {
            true
        };

        if need_read_file {
            let file = File::open(&config.rdf_file)
                .map_err(|x| EmbeddedOxigraphError::ReadNTriplesFileError(x.to_string()))?;
            let mut reader = BufReader::new(file);
            store
                .bulk_loader()
                .load_from_read(RdfParser::from_format(rdf_format).unchecked(), &mut reader)
                .map_err(|x| EmbeddedOxigraphError::LoaderError(x.to_string()))?;
            if let Some(p) = &config.path {
                let mut pb = Path::new(p).to_path_buf();
                pb.push(RDF_FILE_METADATA);
                let mut f = File::create(pb).unwrap();
                write!(f, "{}", rdf_file_metadata).unwrap();
            }
        }
        let oxi = EmbeddedOxigraph { store };
        Ok(oxi)
    }
}

fn file_metadata_string(p: &Path) -> Result<String, std::io::Error> {
    let size = p.size_on_disk()?;
    let changed = p
        .metadata()?
        .created()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Ok(format!("{}_{}", size, changed))
}
