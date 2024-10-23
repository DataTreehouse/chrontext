import pathlib
import time
from multiprocessing import Process

import polars as pl

from chrontext import VirtualizedPythonDatabase, Prefix, XSD, Variable, Template, Parameter, RDFType, Triple, \
    SparqlEmbeddedOxigraph, Engine
from sqlalchemy import MetaData, Table, Column

PATH_HERE = pathlib.Path(__file__).parent.resolve()

class DatabricksDB():
    def __init__(self):
        pass

    def query(self, sql: str):
        self.last_query = sql
        # print(sql)
        ts_1 = pl.read_csv(PATH_HERE / "ts1.csv", try_parse_dates=True).with_columns(
            pl.lit("ts1").alias("id"),
            pl.col("timestamp").dt.replace_time_zone("UTC")
        )
        ts_2 = pl.read_csv(PATH_HERE / "ts2.csv", try_parse_dates=True).with_columns(
            pl.lit("ts2").alias("id"),
            pl.col("timestamp").dt.replace_time_zone("UTC")
        )
        df = pl.concat([ts_1, ts_2]).rename(
            {"timestamp": "t",
             "value": "v",
             "id": "ts_external_id_0"}
        )
        return df


def serve_flight():
    metadata = MetaData()
    table = Table(
        "ts",
        metadata,
        Column("id"),
        Column("timestamp"),
        Column("value")
    )
    vdb = VirtualizedPythonDatabase(
        database=DatabricksDB(),
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
    oxigraph_store = SparqlEmbeddedOxigraph(rdf_file=str(PATH_HERE / "testdata.ttl"), path="oxigraph_db")
    engine = Engine(
        resources,
        virtualized_python_database=vdb,
        sparql_embedded_oxigraph=oxigraph_store)
    engine.init()
    p = Process(target=engine.serve_flight, args=("0.0.0.0:50055",))
    p.start()
    return p

if __name__ == '__main__':
    print("Serving..")
    p = serve_flight()
    time.sleep(10)