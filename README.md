# chrontext: High-performance hybrid query engine for knowledge graphs and time-series
Chrontext allows you to use your knowledge graph to access time-series data. It uses a commodity SPARQL Triplestore and your existing infrastructure for time-series.
It currently supports time-series stored in Google Cloud BigQuery (SQL) and OPC UA HA, but can easily be extended to other APIs and databases.
![Chrontext Architecture](doc/chrontext_arch.png)

Chrontext forms a semantic layer that allows self-service data access, abstracting away technical infrastructure. 
Users can create query-based inputs for data products, that maintains these data products as the knowledge graph is maintained, and that can be deployed across heterogeneous on-premise and cloud infrastructures with the same API. 

Chrontext is a high-performance Python library built in Rust using [Polars](https://www.pola.rs/), and relies heavily on packages from the [Oxigraph](https://github.com/oxigraph/oxigraph) project. 
Chrontext works with [Apache Arrow](https://arrow.apache.org/), prefers time-series transport using [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) and delivers results as [Polars](https://www.pola.rs/) DataFrames.

Please reach out to [Data Treehouse](https://www.data-treehouse.com/contact-8) if you would like help trying Chrontext, or require support for a different time-series backend. 

## Installing
Chrontext is in pip, just use:
```shell
pip install chrontext
```
The API is documented [HERE](https://datatreehouse.github.io/chrontext/chrontext/chrontext.html). 

## Queries in python
We can make queries in Python. The code assumes that we have a SPARQL-endpoint and BigQuery set up with time-series.
```python
from chrontext import *
import os
SCHEMA = os.getenv("SCHEMA")
BIGQUERY_CONN = os.getenv("BIGQUERY_CONN")

tables = [
    TimeseriesTable(
        resource_name="nist",
        schema=SCHEMA,
        time_series_table="nist2",
        value_column="VALUE",
        timestamp_column="TIMESTAMP",
        identifier_column="external_id",
    ),
]
bq_db = TimeseriesBigQueryDatabase(key=os.getenv("BIGQUERY_CONN"), tables=tables)
engine = Engine(timeseries_bigquery_db=bq_db, sparql_endpoint=SPARQL)
engine.init()

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
        DT {
            timestamp= ?t,
            labels= (?ts_pow:"InvPDC_kW"),
            interval= "10m",
            from= "2018-12-25T00:00:00Z",
            aggregation = "avg" }
        }
    ORDER BY ?inv_path ?t
""")
```

This produces the following DataFrame:

| inv_path                    | t                   | ts_pow_value_avg |
|-----------------------------| ---                 | ---              |
| str                         | datetime[ns]        | f64              |
| =\<Metropolis\>.A1.RG1.TBB1 | 2018-12-25 00:00:00 | 0.0              |
| …                           | …                   | …                |
| =\<Metropolis\>.A5.RG9.TBB1 | 2019-01-01 04:50:00 | 0.0              |

Not much power being produced at night in the middle of winter :-)

## API
The API is documented [HERE](https://datatreehouse.github.io/chrontext/chrontext/chrontext.html).

## References
Chrontext is joint work by Magnus Bakken and Professor [Ahmet Soylu](https://www.oslomet.no/om/ansatt/ahmetsoy/) at OsloMet.
To read more about Chrontext, read the article [Chrontext: Portable Sparql Queries Over Contextualised Time Series Data in Industrial Settings](https://www.sciencedirect.com/science/article/pii/S0957417423006516).

## License
All code produced since August 1st. 2023 is copyrighted to [Data Treehouse AS](https://www.data-treehouse.com/) with an Apache 2.0 license unless otherwise noted.

All code which was produced before August 1st. 2023 copyrighted to [Prediktor AS](https://www.prediktor.com/) with an Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree. The code at this state is archived in the repository at [https://github.com/DataTreehouse/chrontext](https://github.com/DataTreehouse/chrontext).