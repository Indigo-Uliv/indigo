"""Indigo Casandra Model

Copyright 2015 Archive Analytics Solutions

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
import dse

import dse.cluster
from dse.cqlengine import connection
from dse.cqlengine.management import (
    create_keyspace_network_topology,
    drop_keyspace,
    sync_table,
    create_keyspace_simple)
from dse.cluster import (
    Cluster, 
    GraphExecutionProfile, 
    EXEC_PROFILE_GRAPH_DEFAULT, 
    EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
)
from dse.graph import GraphOptions
import time

from indigo.util_graph import get_graph_session
from indigo.models.group import Group
from indigo.models.user import User
from indigo.models.tree_entry import TreeEntry
from indigo.models.collection import Collection
from indigo.models.data_object import DataObject
from indigo.models.listener_log import ListenerLog
from indigo.models.resource import Resource
from indigo.models.search import SearchIndex
from indigo.models.id_search import IDSearch
from indigo.models.acl import Ace
from indigo.models.notification import Notification
from indigo.models.graph import Graph

from indigo.log import init_log

logger = init_log('models')



def initialise(keyspace="indigo", hosts=('127.0.0.1',), strategy='SimpleStrategy',
               repl_factor=1):
    """Initialise Cassandra connection"""
    num_retries = 6
    retry_timeout = 1

    for retry in xrange(num_retries):
        try:
            logger.info('Connecting to Cassandra keyspace "{2}" '
                        'on "{0}" with strategy "{1}"'.format(hosts, strategy, keyspace))
            connection.setup(hosts, keyspace, protocol_version=3)

            if strategy is 'SimpleStrategy':
                create_keyspace_simple(keyspace, repl_factor, True)
            else:
                create_keyspace_network_topology(keyspace, {}, True)

            break
        except dse.cluster.NoHostAvailable:
            logger.warning('Unable to connect to Cassandra. Retrying in {0} seconds...'.format(retry_timeout))
            time.sleep(retry_timeout)
            retry_timeout *= 2


def sync():
    """Create tables and graphs for the different models"""
    tables = (User, Group, SearchIndex, IDSearch, TreeEntry, DataObject,
              Notification, ListenerLog)

    for table in tables:
        logger.info('Syncing table "{0}"'.format(table.__name__))
        sync_table(table)
    
    graph_name = 'indigo_graph'
    session = get_graph_session(graph_name)

    session.execute_graph("system.graph(name).ifNotExists().create()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)

    session.execute_graph("schema.config().option('graph.schema_mode').set('Development')")
    session.execute_graph("schema.config().option('graph.allow_scan').set('true')")


def destroy(keyspace):
    """Create Cassandra keyspaces"""
    logger.warning('Dropping keyspace "{0}"'.format(keyspace))
    drop_keyspace(keyspace)
    
    graph_name = 'indigo_graph'
    session = get_graph_session(graph_name)
    
    session.execute_graph("system.graph(name).drop()", {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)


