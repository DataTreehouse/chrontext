import pathlib

import pytest
from SPARQLWrapper import SPARQLWrapper, POST, JSON

from chrontext import Engine, ArrowFlightSQLDatabase, TimeSeriesTable
import polars as pl
from polars.testing import assert_frame_equal


OXIGRAPH_UPDATE_ENDPOINT = "http://127.0.0.1:7878/update"
OXIGRAPH_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
DREMIO_HOST = "127.0.0.1"
DREMIO_PORT = 32010
PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

@pytest.fixture(scope="module")
def oxigraph_testdata(oxigraph_db):
    ep = SPARQLWrapper(OXIGRAPH_UPDATE_ENDPOINT)
    with open(PATH_HERE / "testdata" / "testdata_arrow_flight_sql.sparql") as f:
        query = f.read()
    ep.setMethod(POST)
    ep.setReturnFormat(JSON)
    ep.setQuery(query)
    res = ep.query()
    print(res)

def test_simple_query(dremio_testdata, oxigraph_testdata):
    engine = Engine(OXIGRAPH_QUERY_ENDPOINT)
    tables = [
        TimeSeriesTable(
            schema="my_nas",
            time_series_table="ts.parquet",
            value_column="v",
            timestamp_column="ts",
            identifier_column="id",
            value_datatype="http://www.w3.org/2001/XMLSchema#unsignedInt")
    ]
    arrow_flight_sql_database = ArrowFlightSQLDatabase(host=DREMIO_HOST, port=DREMIO_PORT, username="dremio",
                                                       password="dremio123", tables=tables)
    engine.set_arrow_flight_sql(arrow_flight_sql_database)
    df = engine.execute_hybrid_query("""
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
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
    """)
    expected_csv = TESTDATA_PATH / "expected_simple_query.csv"
    expected_df = pl.read_csv(expected_csv, try_parse_dates=True)
    assert_frame_equal(df, expected_df, check_dtype=False)