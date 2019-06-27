"""Indigo - Project RADON version

Copyright 2019 University of Liverpool

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


from indigo.util_graph import get_graph_session

from dse.cluster import EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT


class Graph(object):
    
    
    def __init__(self, graph_name):
        """Initialize a graph on DSE Graph, creates it if it doesn't exist"""
        self.session = get_graph_session(graph_name)

        self.session.execute_graph("system.graph(name).ifNotExists().create()", 
                                   {'name': graph_name},
                                   execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    
        self.session.execute_graph("schema.config().option('graph.schema_mode').set('Development')")
        self.session.execute_graph("schema.config().option('graph.allow_scan').set('true')")


    def add_edge(self, label, eid, v1_label, v1_id, v2_label, v2_id):
        try:
            # Check if edge exists ?
            query = """v1 = g.V().hasLabel('{}').has('vid', '{}').next();
                       v2 = g.V().hasLabel('{}').has('vid', '{}').next();
                       v1.addEdge('{}', v2, 'eid', {});""".format(
                           v1_label,
                           v1_id,
                           v2_label,
                           v2_id,
                           label,
                           eid)
            self.session.execute_graph(query)
        except Exception as e:
            ## The vertex probaly doesn't exist
            print "problem", query
            return


    def add_vertex(self, label, vid, properties):
        """Add a new vertex with the specified label and the dictionary of 
        properties mapped to properties in the Gremlin graph"""
        # Test if the vertex is already there
        if self.vertex_exists(label, vid):
            # Update ?
            return
        else:
            properties_str = ", ".join(["'{}', '{}'".format(key, value)
                                        for key, value in properties.iteritems()])
            if properties_str:
                query = "graph.addVertex(T.label, '{}', 'vid', {}, {})".format(label, vid, properties_str)
            else:
                query = "graph.addVertex(T.label, '{}', 'vid', {})".format(label, vid)
            print query
            self.session.execute_graph(query)


    def vertex_exists(self, label, vid):
        query = "g.V().hasLabel('{}').has('vid', '{}')".format(label, vid)
        rset = self.session.execute_graph(query)
        try:
            vertex = next(rset.current_rows)
            return True
        except StopIteration:
            return False
    
    



