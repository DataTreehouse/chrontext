import pytest
import polars as pl
import duckdb
import pathlib

from polars.testing import assert_frame_equal
from sqlalchemy import Column, Table, MetaData, literal, bindparam, text

from chrontext import VirtualizedPythonDatabase, Engine, SparqlEmbeddedOxigraph, Template, Prefix, Variable, Parameter, \
    RDFType, xsd, triple
import rdflib

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "parquet_based"
g = rdflib.Graph()
g.parse(TESTDATA_PATH / "testdata.ttl")
g.serialize(TESTDATA_PATH / "testdata.nt", format="nt")

TS1_CSV = str(TESTDATA_PATH / "ts1.csv")
TS2_CSV = str(TESTDATA_PATH / "ts2.csv")


class CSVDB():
    con: duckdb.DuckDBPyConnection

    def __init__(self):
        con = duckdb.connect()
        con.execute("""CREATE TABLE ts1 ("timestamp" TIMESTAMP, "value" INTEGER)""")
        ts_1 = pl.read_csv(TS1_CSV, try_parse_dates=True)
        con.append("ts1", df=ts_1.to_pandas())
        con.execute("""CREATE TABLE ts2 ("timestamp" TIMESTAMP, "value" INTEGER)""")
        ts_2 = pl.read_csv(TS2_CSV, try_parse_dates=True)
        con.append("ts2", df=ts_2.to_pandas())
        self.con = con

    def query(self, sql: str):
        df = self.con.execute(sql).pl()
        print(df)
        return df


@pytest.fixture(scope="function")
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
        resource_sql_map={"my_resource": sql},
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
        "my_resource": Template(
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
    expected = pl.read_csv(
        TESTDATA_PATH / "expected_simple_hybrid.csv", try_parse_dates=True
    ).cast(
        {"v":pl.Int32}
    ).sort(
        by
    )
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


def test_complex_hybrid_query(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w1 ?w2 ?t ?v1 ?v2 WHERE {
        ?w1 a types:BigWidget .
        ?w2 a types:SmallWidget .
        ?w1 types:hasSensor ?s1 .
        ?w2 types:hasSensor ?s2 .
        ?s1 chrontext:hasTimeseries ?ts1 .
        ?s2 chrontext:hasTimeseries ?ts2 .
        ?ts1 chrontext:hasDataPoint ?dp1 .
        ?ts2 chrontext:hasDataPoint ?dp2 .
        ?dp1 chrontext:hasTimestamp ?t .
        ?dp2 chrontext:hasTimestamp ?t .
        ?dp1 chrontext:hasValue ?v1 .
        ?dp2 chrontext:hasValue ?v2 .
        FILTER(?t > "2022-06-01T08:46:55"^^xsd:dateTime && ?v1 < ?v2) .
    }
    """
    by = ["w1", "w2", "t"]
    df = engine.query(q).sort(by)
    expected = pl.read_csv(
        TESTDATA_PATH / "expected_complex_hybrid.csv", try_parse_dates=True).cast(
        {"v1": pl.Int32, "v2": pl.Int32}
    ).sort(by)
    assert_frame_equal(df, expected)
    print(df)


def test_pushdown_group_by_hybrid_query(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime) .
    } GROUP BY ?w
    """
    by = ["w"]
    df = engine.query(q).cast({"sum_v": pl.Int64}).sort(by)
    expected = pl.read_csv(TESTDATA_PATH / "expected_pushdown_group_by_hybrid.csv", try_parse_dates=True).sort(by)
    assert_frame_equal(df, expected)
    print(df)


def test_pushdown_group_by_second_hybrid_query(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(seconds(?t) as ?second)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second
    """
    by = ["w", "sum_v"]
    df = engine.query(q).cast({"sum_v": pl.Int64}).sort(by)
    expected = pl.read_csv(TESTDATA_PATH / "expected_pushdown_group_by_second_hybrid.csv", try_parse_dates=True).sort(by)
    assert_frame_equal(df, expected)

def test_pushdown_group_by_second_having_hybrid_query(engine):
    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (CONCAT(?year, "-", ?month, "-", ?day, "-", ?hour, "-", ?minute, "-", (?second_5*5)) as ?period) (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 5.0)) as ?second_5)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second_5
    HAVING (SUM(?v)>100)
    """
    by = ["w", "period"]
    df = engine.query(q).cast({"sum_v": pl.Int64}).sort(by)
    expected = pl.read_csv(TESTDATA_PATH / "expected_pushdown_group_by_second_having_hybrid.csv", try_parse_dates=True).sort(by)
    assert_frame_equal(df, expected)