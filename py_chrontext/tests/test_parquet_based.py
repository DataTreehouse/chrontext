import pytest
import pathlib
from chrontext import VirtualizedDatabase, Engine, SparqlEmbeddedOxigraph, Template, Prefix, Variable, Parameter, RDFType, xsd, triple
import rdflib

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata" / "parquet_based"
g = rdflib.Graph()
g.parse(TESTDATA_PATH / "testdata.ttl")
g.serialize(TESTDATA_PATH / "testdata.nt", format="nt")


@pytest.fixture()
def engine() -> Engine:
    vdb = VirtualizedDatabase(db_module="my_db")
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
    engine = Engine(vdb, resources, sparql_embedded_oxigraph=oxigraph_store)
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
    df = engine.query(q)
    print(df)