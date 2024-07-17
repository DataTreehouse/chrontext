def test_tutorial():
    import duckdb
    import polars as pl

    class MyDuckDB():
        def __init__(self):
            con = duckdb.connect()
            con.execute("SET TIME ZONE 'UTC';")
            con.execute("""CREATE TABLE ts1 ("timestamp" TIMESTAMPTZ, "value" INTEGER)""")
            ts_1 = pl.read_csv("ts1.csv", try_parse_dates=True).with_columns(
                pl.col("timestamp").dt.replace_time_zone("UTC"))
            con.append("ts1", df=ts_1.to_pandas())
            self.con = con


        def query(self, sql:str) -> pl.DataFrame:
            # We execute the query and return it as a Polars DataFrame.
            # Chrontext expects this method to exist in the provided class.
            df = self.con.execute(sql).pl()
            return df

    my_db = MyDuckDB()

    from sqlalchemy import MetaData, Table, Column, bindparam
    metadata = MetaData()
    ts1_table = Table(
        "ts1", metadata,
        Column("timestamp"), Column("value"))
    sql = ts1_table.select().add_columns(
        bindparam("id1", "ts1").label("id"))

    from chrontext import VirtualizedPythonDatabase

    vdb = VirtualizedPythonDatabase(
        database=my_db,
        resource_sql_map={"my_resource": sql},
        sql_dialect="postgres"
    )

    from chrontext import Prefix, Variable, Template, Parameter, RDFType, Triple, XSD
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
        )}

    from chrontext import Engine, SparqlEmbeddedOxigraph
    oxigraph_store = SparqlEmbeddedOxigraph(rdf_file="my_graph.nt", path="oxigraph_db_tutorial")
    engine = Engine(
        resources,
        virtualized_python_database=vdb,
        sparql_embedded_oxigraph=oxigraph_store)
    engine.init()

    q = """
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s a types:ThingCounter .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53Z"^^xsd:dateTime) .
    } GROUP BY ?w
    """
    df = engine.query(q)
    print(df)

