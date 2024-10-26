from datetime import datetime, date
from typing import List, Dict, Callable, Literal as LiteralType, Union, Optional, Any
from polars import DataFrame
from sqlalchemy import Select, Table

class RDFType:
    """
    The type of a column containing a RDF variable.
    For instance, xsd:string is RDFType.Literal("http://www.w3.org/2001/XMLSchema#string")
    """
    IRI: Callable[[], "RDFType"]
    BlankNode: Callable[[], "RDFType"]
    Literal: Callable[[Union[str, "IRI"]], "RDFType"]
    Multi: Callable[[List["RDFType"]], "RDFType"]
    Nested: Callable[["RDFType"], "RDFType"]
    Unknown: Callable[[], "RDFType"]

class SolutionMappings:
    """
    Detailed information about the solution mappings and the types of the variables.
    """
    mappings: DataFrame
    rdf_types: Dict[str, RDFType]

class Variable:
    """
    A variable in a template.
    """
    name:str

    def __init__(self, name: str):
        """
        Create a new variable.
        :param name: The name of the variable.
        """
        ...


class IRI:
    iri:str
    """
    An IRI.
    """

    def __init__(self, iri: str):
        """
        Create a new IRI
        :param iri: IRI (without < and >).
        """

class BlankNode:
    """
    A Blank Node.
    """
    name: str

    def __init__(self, name: str):
        """
        Create a new Blank Node
        :param name: Name of blank node (without _:).
        """

class Prefix:
    """
    A prefix that can be used to ergonomically build iris.
    """

    def __init__(self, prefix, iri):
        """
        Create a new prefix.
        :param prefix: The name of the prefix
        :param iri: The prefix IRI.
        """

    def suf(self, suffix: str) -> IRI:
        """
        Create a IRI by appending the suffix.
        :param suffix: The suffix to append.
        :return:
        """

class Literal:
    """
    An RDF literal.
    """
    value: str
    data_type: Optional[IRI]
    language: Optional[str]

    def __init__(self, value: str, data_type: IRI = None, language: str = None):
        """
        Create a new RDF Literal
        :param value: The lexical representation of the value.
        :param data_type: The data type of the value (an IRI).
        :param language: The language tag of the value.
        """

    def to_native(self) -> Union[int, float, bool, str, datetime, date]:
        """

        :return:
        """


class Parameter:
    variable: Variable
    optional: bool
    allow_blank: bool
    rdf_type: Optional[RDFType]
    default_value: Optional[Union[Literal, IRI, BlankNode]]
    """
    Parameters for template signatures.
    """

    def __init__(self,
                 variable: Variable,
                 optional: Optional[bool] = False,
                 allow_blank: Optional[bool] = True,
                 rdf_type: Optional[RDFType] = None,
                 default_value: Optional[Union[Literal, IRI, BlankNode]] = None):
        """
        Create a new parameter for a Template.
        :param variable: The variable.
        :param optional: Can the variable be unbound?
        :param allow_blank: Can the variable be bound to a blank node?
        :param rdf_type: The type of the variable. Can be nested.
        :param default_value: Default value when no value provided.
        """


class Argument:
    def __init__(self, term: Union[Variable, IRI, Literal], list_expand: Optional[bool] = False):
        """
        An argument for a template instance.
        :param term: The term.
        :param list_expand: Should the argument be expanded? Used with the list_expander argument of instance.
        """


class Instance:
    def __init__(self,
                 iri: IRI,
                 arguments: List[Union[Argument, Variable, IRI, Literal, BlankNode, None]],
                 list_expander: Optional[LiteralType["cross", "zipMin", "zipMax"]] = None):
        """
        A template instance.
        :param iri: The IRI of the template to be instantiated.
        :param arguments: The arguments for template instantiation.
        :param list_expander: (How) should we do list expansion?
        """


class Template:
    iri: str
    parameters: List[Parameter]
    instances: List[Instance]
    """
    An OTTR Template.
    Note that accessing parameters- or instances-fields returns copies. 
    To change these fields, you must assign new lists of parameters or instances.  
    """

    def __init__(self,
                 iri: IRI,
                 parameters: List[Union[Parameter, Variable]],
                 instances: List[Instance]):
        """
        Create a new OTTR Template
        :param iri: The IRI of the template
        :param parameters:
        :param instances:
        """

    def instance(self,
                 arguments: List[Union[Argument, Variable, IRI, Literal, None]],
                 list_expander: LiteralType["cross", "zipMin", "zipMax"] = None) -> Instance:
        """

        :param arguments: The arguments to the template.
        :param list_expander: (How) should we list-expand?
        :return:
        """

def Triple(subject:Union["Argument", IRI, Variable, BlankNode],
           predicate:Union["Argument", IRI,Variable, BlankNode],
           object:Union["Argument", IRI, Variable, Literal, BlankNode],
           list_expander:Optional[LiteralType["cross", "zipMin", "zipMax"]]=None):
    """
    An OTTR Triple Pattern used for creating templates.
    This is the basis pattern which all template instances are rewritten into.
    Equivalent to:

    >>> ottr = Prefix("http://ns.ottr.xyz/0.4/")
    ... Instance(ottr.suf("Triple"), subject, predicate, object, list_expander)

    :param subject:
    :param predicate:
    :param object:
    :param list_expander:
    :return:
    """

class XSD():
    """
    The xsd namespace, for convenience.
    """
    boolean:IRI
    byte:IRI
    date:IRI
    dateTime:IRI
    dateTimeStamp:IRI
    decimal:IRI
    double:IRI
    duration:IRI
    float:IRI
    int_:IRI
    integer:IRI
    language:IRI
    long:IRI
    short:IRI
    string:IRI

    def __init__(self):
        """
        Create the xsd namespace helper.
        """

def a() -> IRI:
    """
    :return: IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    """

# END COMMON WITH MAPLIB

class VirtualizedPythonDatabase:
    """
    A virtualized database implemented in Python.
    """
    def __init__(self,
                 database: Any,
                 resource_sql_map: Optional[Dict[str, Any]],
                 sql_dialect: Optional[LiteralType["postgres", "bigquery", "databricks"]]):
        """
        See the tutorial in README.md for guidance on how to use this class.
        This API is subject to change, it will be possible to specify what parts of the SPARQL query may be pushed down into the database.
        For advanced use, the resource_sql_map may be omitted, in which case the VirtualizedQuery will be provided to the query method.
        The user must then translate this VirtualizedQuery (built on SPARQL Algebra) to the target query language.

        :param:database: An instance of a class containing a query method.
        :param:resource_sql_map: A dict providing a sqlalchemy Select for each resource.
        :param:sql_dialect: The SQL dialect accepted by the query method.
        """

class VirtualizedBigQueryDatabase:
    """
    A virtualized BigQuery database
    """
    def __init__(self, resource_sql_map: Dict[str, Union[Select, Table]],
                 key_json_path: str):
        """
        To be able to connect to BigQuery, provide the path to the JSON key.
        For each resource name in chrontext that you want to associate with BigQuery,
        provide an sqlalchemy Select or Table that contains each of the parameters
        referenced in the corresponding template provided to Engine.

        See test_bigquery.py in the tests for usage.

        :param resource_sql_map: The SQLs associated with the resources
        :param key_json_path: Path to JSON containing Key to connect to BigQuery.
        """

class VirtualizedOPCUADatabase:
    """
    A virtualized OPC UA Server (History Access), which should be provided to the Engine constructor.
    """
    def __init__(self,
                 namespace: int,
                 endpoint: str):
        """
        Construct new virtualized OPC UA Database.
        See test_opcua.py for an example of use.
        This API is subject to change - will move to URI defined namespaces.

        :param namespace:
        :param endpoint:
        """

class Engine:
    """
    The hybrid query engine of chrontext.
    Initialize Engine using:
        - A SPARQL Database: either in the form of a SPARQL endpoint or an embedded Oxigraph SPARQL database
        - A Virtualized Database: one of the supported databases:
            - A Python defined database (could be anything)
            - Google Cloud BigQuery
            - OPC UA History Access
    """

    def __init__(self,
                 resources: Dict[str, Template],
                 virtualized_python_database: Optional["VirtualizedPythonDatabase"]=None,
                 virtualized_bigquery_database: Optional["VirtualizedBigQueryDatabase"]=None,
                 virtualized_opcua_database: Optional["VirtualizedOPCUADatabase"]=None,
                 sparql_endpoint: Optional[str]=None,
                 sparql_embedded_oxigraph: Optional["SparqlEmbeddedOxigraph"]=None,
        ) -> "Engine":
        """
        Construct a new hybrid query engine.
        Specify exactly one of `sparql_endpoint` and `sparql_embedded_oxigraph`.

        :param resources: The templates associated with each
        :param sparql_endpoint: A SPARQL endpoint (a URL)
        :param sparql_embedded_oxigraph: An embedded oxigraph SPARQL database, see `SparqlEmbeddedOxigraph`.
        """

    def init(self) -> None:
        """
        Initialize the hybrid query engine. 
        
        :return: 
        """

    def query(self,
              query:str,
              native_dataframe:bool=False,
              include_datatypes: bool = False,
              ) -> Union[DataFrame, SolutionMappings]:
        """
        Execute a query

        :param query: The SPARQL query.
        :param native_dataframe: Return columns with chrontext-native formatting. Useful for round-trips into e.g. maplib.
        :param include_datatypes: Return datatypes of the results DataFrame (returns SolutionMappings instead of DataFrame).
        :return: The query result.
        """

    def serve_postgres(self, catalog:"Catalog"):
        """
        Serve the data product catalog as a postgres endpoint.
        Contact DataTreehouse to try.

        :param catalog:
        :return:
        """

    def serve_flight(self, address:str):
        """

        :param address:
        :return:
        """

class SparqlEmbeddedOxigraph:
    """
    Embedded oxigraph SPARQL database, stored in a local folder.
    """

    def __init__(self, rdf_file: str,
                 rdf_format:LiteralType["NTriples", "TTL", "RDF/XML"]=None,
                 path: str=None) -> "SparqlEmbeddedOxigraph":
        """
        Initialize from an RDF file (e.g. ttl or ntriples) or open an embedded SPARQL oxigraph from a path.
        If you want to re-initialize the database, simply delete the folder.

        :param rdf_file: The path of the RDF file that should be loaded into the embedded database.
        :param rdf_format: The format of the RDF file.
        :param path: The path (a folder) where the embedded oxigraph should be stored.
        """

class Catalog:
    """
    A Catalog maps SPARQL queries to virtual SQL tables.
    """
    def __init__(self, data_products:Dict[str, "DataProduct"]):
        """
        Create a new data product catalog, which defines virtual tables.

        :param data_products: The data products in the catalog. Keys are the table names.
        """


class DataProduct:
    """
    A DataProduct is a SPARQL query which is annotated with types.
    It defines a virtual SQL table.
    """
    def __init__(self, query:str, types:Dict[str, RDFType]):
        """
        Create a new data product from a SPARQL query and the types of the columns.
        The SPARQL should be a SELECT query and should explicitly include projected variables (don't use *).

        >>> dp1 = DataProduct(query=query, types={
        ...     "farm_name":RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        ...     "turbine_name":RDFType.Literal("http://www.w3.org/2001/XMLSchema#string"),
        ...     "t":RDFType.Literal("http://www.w3.org/2001/XMLSchema#dateTime"),
        ...     "v":RDFType.Literal("http://www.w3.org/2001/XMLSchema#double")})

        :param query: The SPARQL SELECT Query that defines the data product
        :param types: The types of each of the variables in the data product
        """


class FlightClient:
    def __init__(self, uri:str, metadata:Dict[str, str]=None):
        """

        :param uri: The URI of the Flight server (see engine.serve_flight())
        :param metadata: gRPC metadata to add to each request
        """

    def query(self,
              query:str,
              native_dataframe:bool=False,
              include_datatypes: bool = False,
              ) -> Union[DataFrame, "SolutionMappings"]:
        """
        Execute a query

        :param query: The SPARQL query.
        :param native_dataframe: Return columns with chrontext-native formatting. Useful for round-trips into e.g. maplib.
        :param include_datatypes: Return datatypes of the results DataFrame (returns SolutionMappings instead of DataFrame).
        :return: The query result.
        """