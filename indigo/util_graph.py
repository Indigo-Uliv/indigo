"""Utilities package

Copyright 2017 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


from dse.cluster import (
    Cluster, 
    GraphExecutionProfile, 
    EXEC_PROFILE_GRAPH_DEFAULT, 
    EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
)
from dse.graph import GraphOptions


def get_graph_session(graph_name="indigo_graph"):
    ep = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph_name))
    cluster = Cluster(execution_profiles={EXEC_PROFILE_GRAPH_DEFAULT: ep})
    session = cluster.connect()
    return session


def gq_add_vertex_collection(coll):
    """Create the gremlin query that creates a Vertex for a collection in the
    Tree Entry table
    """
    name = coll.entry.container.split('/')[-1]
    if not name: # root
        name = '/'
    
    query = "graph.addVertex(T.label, 'collection', "
    query += "'name', '{}', ".format(name)
    query += "'uuid', '{}', ".format(coll.uuid)
    query += "'create_ts', '{}', ".format(coll.entry.container_create_ts)
    query += "'modified_ts', '{}')".format(coll.entry.container_modified_ts)
    
    return query


def gq_add_vertex_resource(resc):
    """Create the gremlin query that creates a Vertex for a resource in the
    Tree Entry table
    """
    query = "graph.addVertex(T.label, 'resource', "
    query += "'name', '{}', ".format(resc.get_name())
    query += "'uuid', '{}', ".format(resc.uuid)
    query += "'size', '{}', ".format(resc.get_size())
    #query += "'mimetype', '{}', ".format(resc.get_mimetype() or "application/octet-stream")
    query += "'create_ts', '{}', ".format(resc.get_create_ts())
    query += "'modified_ts', '{}')".format(resc.get_modified_ts())
    return query


def gq_add_vertex_user(user):
    """Create the gremlin query that creates a Vertex for a user
    """
    query = "graph.addVertex(T.label, 'user', "
    query += "'name', '{}', ".format(user.name)
    query += "'uuid', '{}')".format(user.uuid)
    return query



def gq_get_vertex_collection(coll):
    """Create the gremlin query that returns a Vertex for a collection given
    its uuid
    """
    query = "g.V().hasLabel('collection').has('uuid', '{}')".format(coll.uuid)
    return query


def gq_get_metadata_collection(coll):
    """Create the gremlin query that returns metadata for a collection given
    its uuid
    """
    query = """{}.properties();
            """.format(gq_get_vertex_collection(coll))
    return query


def gq_get_metadata_resource(coll):
    """Create the gremlin query that returns metadata for a resource given
    its uuid
    """
    query = """{}.properties();
            """.format(gq_get_vertex_resource(coll))
    return query


def gq_get_vertex_resource(resc):
    """Create the gremlin query that returns a Vertex for a resource given
    its uuid
    """
    query = "g.V().hasLabel('resource').has('uuid', '{}')".format(resc.uuid)
    return query


def gq_get_vertex_user(user):
    """Create the gremlin query that returns a Vertex for a user given
    its uuid
    """
    query = "g.V().hasLabel('user').has('uuid', '{}')".format(user.uuid)
    return query


