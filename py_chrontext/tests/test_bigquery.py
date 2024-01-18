from typing import List

import dotenv
import os

import pytest
from chrontext import *

dotenv.load_dotenv("bq.env")
SCHEMA = os.getenv("SCHEMA")
BIGQUERY_CONN = os.getenv("BIGQUERY_CONN")
print(BIGQUERY_CONN)
skip = SCHEMA is None


@pytest.fixture(scope="function")
def tables() -> List[TimeseriesTable]:
    tables = [
        TimeseriesTable(
            resource_name="nist",
            schema=SCHEMA,
            time_series_table="nist2",
            value_column="VALUE",
            timestamp_column="TIMESTAMP",
            identifier_column="external_id",
            value_datatype="http://www.w3.org/2001/XMLSchema#double",
        ),
        TimeseriesTable(
            resource_name="dataproducts",
            schema=SCHEMA,
            time_series_table="dataproducts",
            value_column="VALUE",
            timestamp_column="TIMESTAMP",
            identifier_column="external_id",
            value_datatype="http://www.w3.org/2001/XMLSchema#double",
        ),
    ]
    return tables


@pytest.fixture(scope="function")
def engine(tables):
    bq_db = TimeseriesBigQueryDatabase(key=BIGQUERY_CONN, tables=tables)
    oxigraph_store = SparqlEmbeddedOxigraph(ntriples_file="solar.nt", path="oxigraph_db")
    engine = Engine(timeseries_bigquery_db=bq_db, sparql_embedded_oxigraph=oxigraph_store)
    engine.init()
    return engine


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(1)
def test_all_timeseries(engine):
    df = engine.query("""
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
    SELECT DISTINCT ?ts_name ?ts_description 
    WHERE {
        ?inv a rds:TBB .
        ?inv ct:hasTimeseries ?ts .
        ?ts rdfs:label ?ts_name .
        ?ts rdfs:comment ?ts_description .
        }
    ORDER BY ASC(?ts_name)
    """)
    assert df.height == 25
    assert df.rdf_datatypes == {'ts_description': '<http://www.w3.org/2001/XMLSchema#string>', 'ts_name': '<http://www.w3.org/2001/XMLSchema#string>'}

@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(2)
def test_get_all_inverters(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?site ?gen_code ?block_code ?inv_code WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Metropolis" .
            ?site rds:functionalAspect+ ?inv .
            ?inv a rds:TBB .
            ?inv rds:code ?inv_code .
            }
        """)
    assert df.height == 50


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(3)
def test_get_inverter_dckw(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?site ?gen_code ?block_code ?inv_code 
               ?year ?month ?day ?hour (xsd:integer(?minute_10) as ?minute) (AVG(?dcpow) as ?avg_dcpow) WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Metropolis" .
            ?site rds:functionalAspect ?block .
            ?block rds:code ?block_code .
            ?block a rds:A .
            ?block rds:functionalAspect ?gen .
            ?gen a rds:RG .
            ?gen rds:code ?gen_code .
            ?gen rds:functionalAspect ?inv .
            ?inv a rds:TBB .
            ?inv rds:code ?inv_code .
            ?inv ct:hasTimeseries ?ts_pow .
            ?ts_pow rdfs:label "InvPDC_kW" .
            ?ts_pow ct:hasDataPoint ?dp_pow .
            ?dp_pow ct:hasTimestamp ?t .
            ?dp_pow ct:hasValue ?dcpow .
            BIND(10 * FLOOR(minutes(?t) / 10.0) as ?minute_10)
            BIND(hours(?t) AS ?hour)
            BIND(day(?t) AS ?day)
            BIND(month(?t) AS ?month)
            BIND(year(?t) AS ?year)
            FILTER(?t > "2018-12-25T00:00:00"^^xsd:dateTime)
            }
        GROUP BY ?site ?block_code ?gen_code ?inv_code ?year ?month ?day ?hour ?minute_10
        ORDER BY ?block_code ?gen_code ?inv_code ?year ?month ?day ?hour ?minute
        """)
    assert df.height == 51900


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_inverter_dckw_sugar(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?site ?gen_code ?block_code ?inv_code WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Metropolis" .
            ?site rds:functionalAspect ?block .
            ?block rds:code ?block_code .
            ?block a rds:A .
            ?block rds:functionalAspect ?gen .
            ?gen a rds:RG .
            ?gen rds:code ?gen_code .
            ?gen rds:functionalAspect ?inv .
            ?inv a rds:TBB .
            ?inv rds:code ?inv_code .
            ?inv ct:hasTimeseries ?ts_pow .
            DT {
                timestamp= ?t,
                labels= (?ts_pow:"InvPDC_kW"),
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?block_code ?gen_code ?inv_code ?t
        """)
    assert df.height == 51900

@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_inverter_dckw_sugar_no_static_results(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?site ?gen_code ?block_code ?inv_code WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Nomatch!!" .
            ?site rds:functionalAspect ?block .
            ?block rds:code ?block_code .
            ?block a rds:A .
            ?block rds:functionalAspect ?gen .
            ?gen a rds:RG .
            ?gen rds:code ?gen_code .
            ?gen rds:functionalAspect ?inv .
            ?inv a rds:TBB .
            ?inv rds:code ?inv_code .
            ?inv ct:hasTimeseries ?ts_pow .
            DT {
                timestamp= ?t,
                labels= (?ts_pow:"InvPDC_kW"),
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?block_code ?gen_code ?inv_code ?t
        """)
    assert df.height == 0

@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_inverter_dckw_sugar_path(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?path WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Metropolis" .
            ?site rds:functionalAspect+ ?inv .
            ?inv a rds:TBB .
            ?inv rds:path ?path .
            ?inv ct:hasTimeseries ?ts_pow .
            DT {
                timestamp= ?t,
                labels= (?ts_pow:"InvPDC_kW"),
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?path ?t
        """)
    assert df.height == 51900


@pytest.mark.skipif(True, reason="Not working yet.. ")
@pytest.mark.order(4)
def test_get_inverter_dckw_sugar_path(engine):
    df = engine.query("""
PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
SELECT ?path WHERE {
    ?site a rds:Site .
    ?site rdfs:label "Jonathanland" .
    ?site rds:functionalAspect ?block .
    ?block a rds:A .
    ?block ct:hasTimeseries ?ts_irr .
    ?block rds:functionalAspect+ ?inv .
    ?inv a rds:TBB .
    ?inv rds:path ?path .
    ?inv ct:hasTimeseries ?ts_pow .
    DT {
        timestamp= ?t,
        labels= (?ts_pow:"InvPDC_kW"),(?ts_irr:"RefCell1_Wm2"),
        interval= "10m",
        from= "2018-12-25T00:00:00Z",
        aggregation = "avg" }
    }
""")
    print(df)
    assert df.height == 51900