import pytest
import polars as pl
import pathlib

from polars.testing import assert_frame_equal
from sqlalchemy import Column, Table, MetaData, literal, bindparam, text

from chrontext import VirtualizedPythonDatabase, Engine, SparqlEmbeddedOxigraph, Template, Prefix, Variable, Parameter, RDFType, xsd, triple
import rdflib

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "parquet_based"
g = rdflib.Graph()
g.parse(TESTDATA_PATH / "testdata.ttl")
g.serialize(TESTDATA_PATH / "testdata.nt", format="nt")

class CSVDB():
    sqlc:pl.SQLContext

    def __init__(self):
        ts1 = pl.read_csv(TESTDATA_PATH / "ts1.csv", try_parse_dates=True)
        ts2 = pl.read_csv(TESTDATA_PATH / "ts2.csv", try_parse_dates=True)
        self.sqlc = pl.SQLContext()
        self.sqlc.register("ts1", ts1)
        self.sqlc.register("ts2", ts2)

    def query(self, sql:str):
        df = self.sqlc.execute(sql, eager=True)
        print(df)
        return df

@pytest.fixture()
def engine() -> Engine:
    timestamp1 = Column("timestamp")
    value1 = Column("value")
    metadata = MetaData()
    ts1_table = Table(
        "ts1",
        metadata,
        timestamp1, value1
    )
    timestamp2 = Column("timestamp")
    value2 = Column("value")
    ts2_table = Table(
        "ts2",
        metadata,
        timestamp2,
        value2
    )
    ts1 = ts1_table.select().add_columns(
        bindparam("id1", "ts1").label("id"),
    )
    ts2 = ts2_table.select().add_columns(
        bindparam("id2", "ts2").label("id"),
    )
    sql = ts1.union(ts2)

    vdb = VirtualizedPythonDatabase(
        database=CSVDB(),
        resource_sql_map={"my_resource":sql},
        sql_dialect="postgres"
    )

    oxigraph_store = SparqlEmbeddedOxigraph(ntriples_file=str(TESTDATA_PATH / "testdata.nt"), path="oxigraph_db")
    ct = Prefix("ct", "https://github.com/DataTreehouse/chrontext#")
    x = xsd()
    id = Variable("id")
    timestamp = Variable("timestamp")
    value = Variable("value")
    dp = Variable("dp")
    resources = {
        "my_resource":Template(
            iri=ct.suf("my_resource"),
            parameters=[
                Parameter(id, rdf_type=RDFType.Literal(x.string)),
                Parameter(timestamp, rdf_type=RDFType.Literal(x.dateTime)),
                Parameter(value, rdf_type=RDFType.Literal(x.double)),
            ],
            instances=[
                triple(id, ct.suf("hasDataPoint"), dp),
                triple(dp, ct.suf("hasValue"), value),
                triple(dp, ct.suf("hasTimestamp"), timestamp)
            ]
        )
    }
    engine = Engine(
        resources,
        virtualized_python_database=vdb,
        sparql_embedded_oxigraph=oxigraph_store)
    engine.init()
    return engine

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
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    """
    by = ["w", "s", "t"]
    df = engine.query(q).sort(by)
    expected = pl.read_csv(TESTDATA_PATH / "expected_simple_hybrid.csv", try_parse_dates=True).sort(by)
    assert_frame_equal(df, expected)
    print(df)


def test_simple_hybrid_no_vq_matches_query(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidgetInvalidType .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    """
    df = engine.query(q)
    assert df.height == 0