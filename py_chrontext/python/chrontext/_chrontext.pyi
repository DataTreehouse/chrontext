from typing import List
from .semantic_dataframe import SemanticDataFrame
class Engine:
    """
    The hybrid query engine of chrontext.
    Initialize Engine using:
        - A SPARQL Database: either in the form of a SPARQL endpoint or an embedded Oxigraph SPARQL database
        - A Timeseries Database: one of the supported databases: Google Cloud BigQuery or OPC UA HA
    """

    def __init__(self,
                 sparql_endpoint: str=None,
                 sparql_embedded_oxigraph: SparqlEmbeddedOxigraph=None,
                 timeseries_bigquery_db: TimeseriesBigQueryDatabase=None,
                 timeseries_opcua_db: TimeseriesOPCUADatabase=None) -> Engine:
        """
        Construct a new hybrid query engine.
        Specify exactly one of `sparql_endpoint` and `sparql_embedded_oxigraph`.
        Specify exactly one of `timeseries_bigquery_db` and `timeseries_opcua_db`

        :param sparql_endpoint: A SPARQL endpoint (a URL)
        :param sparql_embedded_oxigraph: An embedded oxigraph SPARQL database, see `SparqlEmbeddedOxigraph`.
        :param timeseries_bigquery_db: A BigQuery database containing timeseries data, see: `TimeseriesBigQueryDatabase`
        :param timeseries_opcua_db: A OPC UA HA endpoint containing timeseries data, see: `TimeseriesOPCUADatabase`
        """
        self.sparql_endpoint = sparql_endpoint
        self.sparql_embedded_oxigraph = sparql_embedded_oxigraph
        self.timeseries_bigquery_db = timeseries_bigquery_db
        self.timeseries_opcua_db = timeseries_opcua_db


    def init(self) -> None:
        """
        Initialize the hybrid query engine. 
        
        :return: 
        """

    def query(self, query:str) -> SemanticDataFrame:
        """

        :param query: The SPARQL query.
        :return: The query result.
        """

class SparqlEmbeddedOxigraph:
    """
    Embedded oxigraph SPARQL database, stored in a local folder.
    """

    def __init__(self, ntriples_file: str, path: str=None) -> SparqlEmbeddedOxigraph:
        """
        Initialize from NTriples or open an embedded SPARQL oxigraph from a path.
        If you want to re-initialize the database, simply delete the folder.

        :param ntriples_file: The path of the NTriples (.nt) file that should be loaded into the embedded database.
        :param path: The path (a folder) where the embedded oxigraph should be stored.
        """

class TimeseriesOPCUADatabase:
    """
    OPC UA History Access Endpoint.
    """
    def __init__(self) -> TimeseriesOPCUADatabase:
        """
        Initialize the OPC UA History Access endpoint.
        """

class TimeseriesBigQueryDatabase:
    """
    BigQuery containing timeseries data.
    """

    def __init__(self, tables: List[TimeseriesTable], key: str) -> TimeseriesBigQueryDatabase:
        """
        Create a new mapping to a BigQuery database containing timeseries data.

        :param tables: The referenced tables.
        :param key: Path to json key with credentials. This will soon change.
        """


class TimeseriesTable:
    """
    TimeseriesTable, which maps an SQL table to the chrontext predicates,
    so that they can be queried as part of a virtual knowledge graph.
    """
    def __init__(self, resource_name: str,
                 time_series_table: str,
                 value_column: str,
                 timestamp_column: str,
                 identifier_column: str,
                 schema: str=None,
                 year_column: str=None,
                 month_column: str=None,
                 day_column: str=None) -> TimeseriesTable:
        """
        Create a new TimeseriesTable, which maps an SQL table to the chrontext predicates.

        :param resource_name: Name of the resource. This is the object of the `ct:hasResource`-property.
        :param schema: In BigQuery, this is the data set id, otherwise the name of the database schema.
        :param time_series_table: Table containing timeseries data.
        :param value_column: Column containing the values. This is the object of the `ct:hasValue`-property.
        :param timestamp_column: Column containing the timestamps. This is the object of the `ct:hasTimestamp`-property.
        :param identifier_column: Column containing the identity of the timeseries. This is the object of the `ct:hasExternalId`-property.
        :param year_column: Optionally the column containing the year of the timestamp, used for parititioning.
        :param month_column: Optionally the column containing the day of the timestamp, used for parititioning.
        :param day_column: Optionally the column containing the day of the timestamp, used for parititioning.
        """
