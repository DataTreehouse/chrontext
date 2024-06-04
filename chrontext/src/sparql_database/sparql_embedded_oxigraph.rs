use super::SparqlQueryable;
use async_trait::async_trait;
use filesize::PathExt;
use oxigraph::io::DatasetFormat;
use oxigraph::sparql::QueryResults;
use oxigraph::store::Store;
use serde::{Deserialize, Serialize};
use sparesults::QuerySolution;
use spargebra::Query;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{read_to_string, File};
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::time::SystemTime;

const TTL_FILE_METADATA: &str = "ttl_file_data.txt";

#[derive(Debug)]
pub enum EmbeddedOxigraphError {
    OpenStorageError(String),
    ReadNTriplesFileError(String),
    LoaderError(String),
    DBMetadataIOError(String),
}

impl Display for EmbeddedOxigraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EmbeddedOxigraphError::OpenStorageError(o) => {
                write!(f, "Error opening oxigraph storage path {}", o)
            }
            EmbeddedOxigraphError::ReadNTriplesFileError(s) => {
                write!(f, "Error reading NTriples file {}", s)
            }
            EmbeddedOxigraphError::LoaderError(s) => {
                write!(f, "Error reading loading NTriples file {}", s)
            }
            EmbeddedOxigraphError::DBMetadataIOError(s) => {
                write!(f, "Oxigraph metadata IO Error {}", s)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbeddedOxigraphConfig {
    pub path: Option<String>,
    pub ntriples_file: String,
}

pub struct EmbeddedOxigraph {
    pub store: Store,
}

#[async_trait]
impl SparqlQueryable for EmbeddedOxigraph {
    async fn execute(&self, query: &Query) -> Result<Vec<QuerySolution>, Box<dyn Error>> {
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

impl EmbeddedOxigraph {
    pub fn from_config(
        config: EmbeddedOxigraphConfig,
    ) -> Result<EmbeddedOxigraph, EmbeddedOxigraphError> {
        let ntriples_path = Path::new(&config.ntriples_file);
        let ntriples_file_metadata = file_metadata_string(ntriples_path)
            .map_err(|x| EmbeddedOxigraphError::DBMetadataIOError(x.to_string()))?;

        let store = if let Some(p) = &config.path {
            Store::open(Path::new(p))
        } else {
            Store::new()
        }
        .map_err(|x| EmbeddedOxigraphError::OpenStorageError(x.to_string()))?;

        let need_read_file = if let Some(p) = &config.path {
            let mut pb = Path::new(p).to_path_buf();
            pb.push(Path::new(TTL_FILE_METADATA));
            let dbdata_path = pb.as_path();
            if dbdata_path.exists() {
                let existing_db_ntriples_metadata = read_to_string(dbdata_path)
                    .map_err(|x| EmbeddedOxigraphError::DBMetadataIOError(x.to_string()))?;
                existing_db_ntriples_metadata != ntriples_file_metadata
            } else {
                true
            }
        } else {
            true
        };

        if need_read_file {
            let file = File::open(&config.ntriples_file)
                .map_err(|x| EmbeddedOxigraphError::ReadNTriplesFileError(x.to_string()))?;
            let reader = BufReader::new(file);
            store
                .bulk_loader()
                .load_dataset(reader, DatasetFormat::NQuads, None)
                .map_err(|x| EmbeddedOxigraphError::LoaderError(x.to_string()))?;
            if let Some(p) = &config.path {
                let mut pb = Path::new(p).to_path_buf();
                pb.push(TTL_FILE_METADATA);
                let mut f = File::create(pb).unwrap();
                write!(f, "{}", ntriples_file_metadata).unwrap();
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
