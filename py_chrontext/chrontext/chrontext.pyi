from typing import List, Dict, Callable, Literal as LiteralType, Union
from polars import DataFrame


class VirtualizedDatabase:
    """
    A virtualized database implemented in Python.
    """
    def __init__(self, db_module: str):
        """

        :param db_module:
        """

class RDFType:
    """
    The type of a column containing a RDF variable.
    """
    IRI: Callable[[], "RDFType"]
    Blank: Callable[[], "RDFType"]
    Literal: Callable[[str], "RDFType"]
    Nested: Callable[["RDFType"], "RDFType"]
    Unknown: Callable[[], "RDFType"]


class Variable:
    """
    A variable in a template.
    """

    def __init__(self, name: str):
        """
        Create a new variable.
        :param name: The name of the variable.
        """
        ...


class IRI:
    """
    An IRI.
    """

    def __init__(self, iri: str):
        """
        Create a new IRI
        :param iri: IRI (without < and >).
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

    def __init__(self, value: str, data_type: IRI = None, language: str = None):
        """
        Create a new RDF Literal
        :param value: The lexical representation of the value.
        :param data_type: The data type of the value (an IRI).
        :param language: The language tag of the value.
        """


class Parameter:
    def __init__(self,
                 variable: Variable,
                 optional: bool = False,
                 allow_blank: bool = True,
                 rdf_type: RDFType = None):
        """
        Create a new parameter.
        :param variable: The variable.
        :param optional: Can the variable be unbound?
        :param allow_blank: Can the variable be bound to a blank node?
        :param rdf_type: The type of the variable. Can be nested.
        """


class Argument:
    def __init__(self, term: Union[Variable, IRI, Literal], list_expand: bool):
        """
        An argument for a template instance.
        :param term: The term.
        :param list_expand: Should the argument be expanded? Used with the list_expander argument of instance.
        """


class Instance:
    def __init__(self,
                 iri: IRI,
                 arguments: List[Union[Argument, Variable, IRI, Literal]],
                 list_expander: LiteralType["cross", "zipMin", "zipMax"] = None):
        """
        A template instance.
        :param iri: The IRI of the template to be instantiated.
        :param arguments: The arguments for template instantiation.
        :param list_expander: (How) should we do list expansion?
        """


class Template:
    """
    An OTTR Template.
    """

    def __init__(self,
                 iri: IRI,
                 parameters: List[Parameter],
                 instances: List[Instance]):
        """
        Create a new OTTR Template
        :param iri: The IRI of the template
        :param parameters:
        :param instances:
        """

    def instance(self,
                 arguments: List[Union[Argument, Variable, IRI, Literal]],
                 list_expander: LiteralType["cross", "zipMin", "zipMax"] = None) -> Instance:
        """

        :param arguments: The arguments to the template.
        :param list_expander: (How) should we list-expand?
        :return:
        """

class Engine:
    """
    The hybrid query engine of chrontext.
    Initialize Engine using:
        - A SPARQL Database: either in the form of a SPARQL endpoint or an embedded Oxigraph SPARQL database
        - A Timeseries Database: one of the supported databases: Google Cloud BigQuery or OPC UA HA
    """

    def __init__(self,
                 virtualized_database: VirtualizedDatabase,
                 resources: Dict[str, Template],
                 sparql_endpoint: str=None,
                 sparql_embedded_oxigraph: SparqlEmbeddedOxigraph=None,
        ) -> Engine:
        """
        Construct a new hybrid query engine.
        Specify exactly one of `sparql_endpoint` and `sparql_embedded_oxigraph`.

        :param sparql_endpoint: A SPARQL endpoint (a URL)
        :param sparql_embedded_oxigraph: An embedded oxigraph SPARQL database, see `SparqlEmbeddedOxigraph`.
        """

    def init(self) -> None:
        """
        Initialize the hybrid query engine. 
        
        :return: 
        """

    def query(self, query:str, native_dataframe:bool=False) -> DataFrame:
        """
        Execute a query

        :param query: The SPARQL query.
        :param native_dataframe: Return columns with chrontext-native formatting. Useful for round-trips into e.g. maplib.
        :return: The query result.
        """

    def serve_postgres(self, catalog:Catalog):
        """

        :param catalog:
        :return:
        """

class SparqlEmbeddedOxigraph:
    """
    Embedded oxigraph SPARQL database, stored in a local folder.
    """

    def __init__(self, ntriples_file: str, path: str=None) -> SparqlEmbeddedOxigraph:
        """
        Initialize from NTriples or open an embedded SPARQL oxigraph from a path.
        If you want to re-initialize the database, simply delete the folder.

        :param ntriples_file: The path of the NTriples (.nt) file that should be loaded into the embedded database.
        :param path: The path (a folder) where the embedded oxigraph should be stored.
        """

class Catalog:
    """
    A Catalog maps SPARQL queries to virtual SQL tables.
    """
    def __init__(self, data_products:Dict[str, DataProduct]):
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

class RDFType:
    """
    The type of a column containing a RDF variable.
    """
    NamedNode:Callable[[], RDFType]
    BlankNode:Callable[[], RDFType]
    Literal:Callable[[str], RDFType]
    Unknown:Callable[[], RDFType]

