"""
RayDB - High-performance embedded graph database with vector search

A Python interface to the RayDB graph database, providing:
- ACID transactions
- Node and edge CRUD operations
- Property storage
- Vector embeddings with IVF/IVF-PQ indexes
- Graph traversal and pathfinding (BFS, Dijkstra)

Example:
    >>> from raydb import Database, PropValue
    >>> 
    >>> # Open or create a database
    >>> with Database("my_graph.raydb") as db:
    ...     # Start a transaction
    ...     db.begin()
    ...     
    ...     # Create nodes
    ...     alice = db.create_node("user:alice")
    ...     bob = db.create_node("user:bob")
    ...     
    ...     # Set properties
    ...     name_key = db.get_or_create_propkey("name")
    ...     db.set_node_prop(alice, name_key, PropValue.string("Alice"))
    ...     db.set_node_prop(bob, name_key, PropValue.string("Bob"))
    ...     
    ...     # Create an edge
    ...     knows = db.get_or_create_etype("knows")
    ...     db.add_edge(alice, knows, bob)
    ...     
    ...     # Traverse the graph
    ...     friends = db.traverse_out(alice, knows)
    ...     
    ...     # Find shortest path
    ...     path = db.find_path_bfs(alice, bob)
    ...     
    ...     # Commit changes
    ...     db.commit()
"""

from raydb._raydb import (
    # Core classes
    Database,
    OpenOptions,
    SyncMode,
    DbStats,
    CacheStats,
    PropValue,
    Edge,
    FullEdge,
    NodeProp,
    
    # Traversal result classes
    TraversalResult,
    PathResult,
    PathEdge,
    
    # Vector search classes
    IvfIndex,
    IvfPqIndex,
    IvfConfig,
    PqConfig,
    SearchOptions,
    SearchResult,
    IvfStats,
    
    # Functions
    open_database,
    version,
    brute_force_search,
)

__version__ = version()
__all__ = [
    # Core
    "Database",
    "OpenOptions",
    "SyncMode",
    "DbStats",
    "CacheStats",
    "PropValue",
    "Edge",
    "FullEdge",
    "NodeProp",
    
    # Traversal
    "TraversalResult",
    "PathResult",
    "PathEdge",
    
    # Vector
    "IvfIndex",
    "IvfPqIndex",
    "IvfConfig",
    "PqConfig",
    "SearchOptions",
    "SearchResult",
    "IvfStats",
    
    # Functions
    "open_database",
    "version",
    "brute_force_search",
    
    # Version
    "__version__",
]
