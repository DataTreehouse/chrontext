from typing import List

import dotenv
import os

import pytest
from chrontext import *

dotenv.load_dotenv("bq.env")
SCHEMA = os.getenv("SCHEMA")
BIGQUERY_CONN = os.getenv("BIGQUERY_CONN")
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
        ),
        TimeseriesTable(
            resource_name="dataproducts",
            schema=SCHEMA,
            time_series_table="dataproducts",
            value_column="VALUE",
            timestamp_column="TIMESTAMP",
            identifier_column="external_id",
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
    assert df.columns == ['ts_name', 'ts_description']
    assert df.height == 25
    # TODO: Fix datatypes
    # assert df.rdf_datatypes == {'ts_description': '<http://www.w3.org/2001/XMLSchema#string>', 'ts_name': '<http://www.w3.org/2001/XMLSchema#string>'}


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
    assert ['site', 'gen_code', 'block_code', 'inv_code']
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
    assert (df.columns ==
            ['site', 'gen_code', 'block_code', 'inv_code', 'year', 'month', 'day', 'hour', 'minute', 'avg_dcpow'])
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
            ?ts_pow rdfs:label "InvPDC_kW" .
            DT {
                timestamp= ?t,
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?block_code ?gen_code ?inv_code ?t
        """)
    #print(df)
    assert df.columns == ['site', 'gen_code', 'block_code', 'inv_code', 't', 'ts_pow_value_avg']
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
            ?ts_pow rdfs:label "InvPDC_kW" .
            DT {
                timestamp= ?t,
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?block_code ?gen_code ?inv_code ?t
        """)
    #print(df)
    assert df.columns == ['site', 'gen_code', 'block_code', 'inv_code', 't', 'ts_pow_value_avg']
    assert df.height == 51900


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_simplified_inverter_dckw_sugar(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
    SELECT ?inv_path WHERE {
        # We are navigating th Solar PV site "Metropolis", identifying every inverter. 
        ?site a rds:Site .
        ?site rdfs:label "Metropolis" .
        ?site rds:functionalAspect+ ?inv .    
        ?inv a rds:TBB .                    # RDS code TBB: Inverter
        ?inv rds:path ?inv_path .
        
        # Find the timeseries associated with the inverter
        ?inv ct:hasTimeseries ?ts_pow .
        ?ts_pow rdfs:label "InvPDC_kW" .
        DT {
            timestamp = ?t,
            timeseries = ?ts_pow,
            interval = "10m",
            from = "2018-12-25T00:00:00Z",
            aggregation = "avg" }
        }
    ORDER BY ?inv_path ?t
        """)
    assert df.columns == ['inv_path', 't', 'ts_pow_value_avg']
    assert df.height == 51900

@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_simplified_inverter_dckw_sugar_no_dynamic_results(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
    SELECT ?inv_path WHERE {
        # We are navigating th Solar PV site "Metropolis", identifying every inverter. 
        ?site a rds:Site .
        ?site rdfs:label "Metropolis" .
        ?site rds:functionalAspect+ ?inv .    
        ?inv a rds:TBB .                    # RDS code TBB: Inverter
        ?inv rds:path ?inv_path .
        
        # Find the timeseries associated with the inverter
        ?inv ct:hasTimeseries ?ts_pow .
        ?ts_pow rdfs:label "InvPDC_kW" .
        DT {
            timestamp = ?t,
            timeseries = ?ts_pow,
            interval = "10m",
            from = "2028-12-25T00:00:00Z",
            aggregation = "avg" }
        }
    ORDER BY ?inv_path ?t
        """)
    assert df.columns == ['inv_path', 't', 'ts_pow_value_avg']
    assert df.height == 0

@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(4)
def test_get_simplified_inverter_dckw_sugar_multiagg(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
    PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
    SELECT ?inv_path WHERE {
        # We are navigating th Solar PV site "Metropolis", identifying every inverter. 
        ?site a rds:Site .
        ?site rdfs:label "Metropolis" .
        ?site rds:functionalAspect+ ?inv .    
        ?inv a rds:TBB .                    # RDS code TBB: Inverter
        ?inv rds:path ?inv_path .
        
        # Find the timeseries associated with the inverter
        ?inv ct:hasTimeseries ?ts_pow .
        ?ts_pow rdfs:label "InvPDC_kW" .
        DT {
            timestamp = ?t,
            timeseries = ?ts_pow,
            interval = "10m",
            from = "2018-12-25T00:00:00Z",
            aggregation = "avg", "min" }
        }
    ORDER BY ?inv_path ?t
        """)
    assert df.columns == ['inv_path', 't', 'ts_pow_value_avg', 'ts_pow_value_min']
    assert df.height == 51900



@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(5)
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
            ?ts_pow rdfs:label "InvPDC_kW" .
            DT {
                timestamp= ?t,
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?block_code ?gen_code ?inv_code ?t
        """)
    assert df.columns == ['site', 'gen_code', 'block_code', 'inv_code', 't', 'ts_pow_value_avg']
    assert df.height == 0


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(6)
def test_get_inverter_dckw_sugar_path(engine):
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
        PREFIX rds: <https://github.com/DataTreehouse/solar_demo/rds_power#> 
        SELECT ?path ?inv WHERE {
            ?site a rds:Site .
            ?site rdfs:label "Metropolis" .
            ?site rds:functionalAspect+ ?inv .
            ?inv a rds:TBB .
            ?inv rds:path ?path .
            ?inv ct:hasTimeseries ?ts_pow .
            ?ts_pow rdfs:label "InvPDC_kW" .
            DT {
                timestamp= ?t,
                interval= "10m",
                from= "2018-12-25T00:00:00Z",
                aggregation = "avg" }
            }
        ORDER BY ?path ?t
        """)

    assert df.columns == ['path', 'inv', 't', 'ts_pow_value_avg']
    assert df.height == 51900


@pytest.mark.skipif(skip, reason="Environment vars not present")
@pytest.mark.order(7)
def test_get_inverter_dckw_sugar_multi(engine):
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
    ?ts_irr rdfs:label "RefCell1_Wm2" .
    ?block rds:functionalAspect+ ?inv .
    ?inv a rds:TBB .
    ?inv rds:path ?path .
    ?inv ct:hasTimeseries ?ts_pow .
    ?ts_pow rdfs:label "InvPDC_kW" .
    DT {
        timestamp = ?t,
        timeseries = ?ts_pow, ?ts_irr,
        interval= "10m",
        from= "2018-12-25T00:00:00Z",
        aggregation = "avg", "min" }
    }
""")
    #print(df)
    assert df.columns == ['path',
                          't',
                          'ts_pow_value_avg',
                          'ts_pow_value_min',
                          'ts_irr_value_avg',
                          'ts_irr_value_min']
    assert df.height == 51900
