import asyncio
import os
import pathlib
import time
from multiprocessing import Process
from typing import Dict

import polars as pl
from polars.testing import assert_frame_equal

import pytest
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from asyncua import Server, ua
from asyncua.server.history_sql import HistorySQLite
from asyncua.ua import NodeId, DataValue, Variant
from datetime import datetime

from chrontext import *

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"
HISTORY_DB_PATH = PATH_HERE / "history.db"

OXIGRAPH_UPDATE_ENDPOINT = "http://127.0.0.1:7878/update"
OXIGRAPH_QUERY_ENDPOINT = "http://127.0.0.1:7878/query"
OPCUA_ENDPOINT = "opc.tcp://127.0.0.1:4841/freeopcua/server/"

async def start_opcua_server():
    # setup our server
    server = Server()

    db = HistorySQLite(str(HISTORY_DB_PATH))
    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(db)

    # initialize server
    await server.init()

    server.set_endpoint("opc.tcp://0.0.0.0:4841/freeopcua/server/")
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity])

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = await server.register_namespace(uri)
    print("Namespace index: ", idx)

    # get Objects node, this is where we should put our custom stuff
    objects = server.nodes.objects

    # populating our address space
    var1 = await objects.add_variable(NodeId("ts1", idx), "Timeseries1", 0)
    var2 = await objects.add_variable(NodeId("ts2", idx), "Timeseries2", 0)

    # starting!
    await server.start()

    # enable data change history for this particular node, must be called after start since it uses subscription
    await server.historize_node_data_change(var1, period=None, count=0)
    await server.historize_node_data_change(var2, period=None, count=0)

    for c in range(60):
        await var1.write_value(DataValue(Value=Variant(100 + c), SourceTimestamp=datetime(2022, 8, 17, 10, 42, c)))
        await var2.write_value(DataValue(Value=Variant(200 + c), SourceTimestamp=datetime(2022, 8, 17, 10, 42, c)))

    #Necessary for the server to stay alive
    await asyncio.sleep(1000)


#Based on example from https://github.com/FreeOpcUa/opcua-asyncio/blob/master/examples/server-datavalue-history.py
@pytest.fixture
def opcua_server():
    if os.path.exists(HISTORY_DB_PATH):
        os.remove(HISTORY_DB_PATH)
    p = Process(
        target=asyncio.run, args=(start_opcua_server(),), daemon=True)
    p.start()
    time.sleep(10)


@pytest.fixture(scope="module")
def oxigraph_testdata(oxigraph_db):
    ep = SPARQLWrapper(OXIGRAPH_UPDATE_ENDPOINT)
    with open(PATH_HERE / "testdata" / "testdata_opcua_history_read.sparql") as f:
        query = f.read()
    ep.setMethod(POST)
    ep.setReturnFormat(JSON)
    ep.setQuery(query)
    res = ep.query()
    #print(res)

@pytest.fixture(scope="module")
def resources() -> Dict[str, Template]:
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
    return resources

@pytest.mark.skip("Unknown issue running this test in github actions, needs triage")
def test_simplified_opcua_case(opcua_server, oxigraph_testdata, resources):
    print("Begin test")
    opcua_db = VirtualizedOPCUADatabase(namespace=2, endpoint=OPCUA_ENDPOINT)
    print("created opcua backend")
    engine = Engine(resources=resources, sparql_endpoint=OXIGRAPH_QUERY_ENDPOINT, virtualized_opcua_database=opcua_db)
    print("defined engine")
    df = engine.query("""
        PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
        PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
        PREFIX types:<http://example.org/types#>
        SELECT ?w ?s ?mytype ?t ?v WHERE {
            ?w a ?mytype .
            ?w types:hasSensor ?s .
            ?s chrontext:hasTimeseries ?ts .
            ?ts chrontext:hasDataPoint ?dp .
            ?dp chrontext:hasTimestamp ?t .
            ?dp chrontext:hasValue ?v .
            FILTER(?t < "2022-08-17T16:46:53"^^xsd:dateTime && ?v > 150.0) .
        }
        """)
    expected_csv = TESTDATA_PATH / "expected_simplified_opcua_case.csv"
    #df.write_csv(expected_csv)
    expected_df = pl.read_csv(expected_csv, try_parse_dates=True)
    expected_df = expected_df.sort(["w", "s", "mytype", "t", "v"])
    df = df.sort(["w", "s", "mytype", "t", "v"])
    assert_frame_equal(df, expected_df, check_dtype=False)
