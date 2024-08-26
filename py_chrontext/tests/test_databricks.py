import pytest
import polars as pl
import duckdb
import pathlib

from polars.testing import assert_frame_equal
from sqlalchemy import Column, Table, MetaData, bindparam

from chrontext import VirtualizedPythonDatabase, Engine, SparqlEmbeddedOxigraph, Template, Prefix, Variable, Parameter, \
    RDFType, XSD, Triple

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "python_based"
TS1_CSV = str(TESTDATA_PATH / "ts1.csv")
TS2_CSV = str(TESTDATA_PATH / "ts2.csv")


class DatabricksDB():
    def __init__(self):
        pass

    def query(self, sql: str):
        self.last_query = sql
        #print(sql)
        ts_1 = pl.read_csv(TS1_CSV, try_parse_dates=True).with_columns(
            pl.lit("ts1").alias("id"),
            pl.col("timestamp").dt.replace_time_zone("UTC")
        )
        ts_2 = pl.read_csv(TS2_CSV, try_parse_dates=True).with_columns(
            pl.lit("ts2").alias("id"),
            pl.col("timestamp").dt.replace_time_zone("UTC")
        )
        df = pl.concat([ts_1, ts_2]).rename(
            {"timestamp":"t",
             "value":"v",
             "id":"ts_external_id_0"}
        )
        return df

db = DatabricksDB()

@pytest.fixture(scope="module")
def engine() -> Engine:
    metadata = MetaData()
    table = Table(
        "ts",
        metadata,
        Column("id"),
        Column("timestamp"),
        Column("value")
    )
    vdb = VirtualizedPythonDatabase(
        database=db,
        resource_sql_map={"my_resource": table},
        sql_dialect="databricks"
    )

    ct = Prefix("ct", "https://github.com/DataTreehouse/chrontext#")
    xsd = XSD()
    id = Variable("id")
    timestamp = Variable("timestamp")
    value = Variable("value")
    dp = Variable("dp")
    resources = {
        "my_resource": Template(
            iri=ct.suf("my_resource"),
            parameters=[
                Parameter(id, rdf_type=RDFType.Literal(xsd.string)),
                Parameter(timestamp, rdf_type=RDFType.Literal(xsd.dateTime)),
                Parameter(value, rdf_type=RDFType.Literal(xsd.double)),
            ],
            instances=[
                Triple(id, ct.suf("hasDataPoint"), dp),
                Triple(dp, ct.suf("hasValue"), value),
                Triple(dp, ct.suf("hasTimestamp"), timestamp)
            ]
        )
    }
    oxigraph_store = SparqlEmbeddedOxigraph(rdf_file=str(TESTDATA_PATH / "testdata.ttl"), path="oxigraph_db")
    engine = Engine(
        resources,
        virtualized_python_database=vdb,
        sparql_embedded_oxigraph=oxigraph_store)
    engine.init()
    return engine


@pytest.mark.order(1)
def test_simple_hybrid(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53Z"^^xsd:dateTime && ?v < 200) .
    }
    """
    engine.query(q, include_datatypes=True)
    sql = db.last_query
    assert sql.startswith("""SELECT inner_0.id AS ts_external_id_0""")