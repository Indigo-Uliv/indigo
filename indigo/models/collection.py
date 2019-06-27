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

from datetime import datetime
from dse.cqlengine import connection
from dse.query import SimpleStatement
import json

from indigo import (
    RESERVED_META,
    get_config
)
from indigo.util_graph import (
    get_graph_session,
    gq_add_vertex_collection,
    gq_get_metadata_collection,
    gq_get_vertex_collection,
    gq_get_vertex_user,
)
from indigo.models import (
    TreeEntry,
    User
)
from indigo.models.acl import (
    acemask_to_str,
    serialize_acl_metadata
)
from indigo.util import (
    datetime_serializer,
    decode_meta,
    meta_cassandra_to_cdmi,
    meta_cdmi_to_cassandra,
    metadata_to_list,
    merge,
    split,
)
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)

# import logging


class Collection(object):
    """Collection model"""

    def __init__(self, entry):
        self.entry = entry

        self.is_root = (self.entry.name == "." and self.entry.container == '/')
        # Get name
        if self.is_root:
            self.name = u"Home"
        else:
            _, self.name = split(self.entry.container)
        self.path = self.entry.container
        self.container, _ = split(self.path)
        self.uuid = self.entry.uuid
        self.create_ts = self.entry.container_create_ts


    @classmethod
    def check_graph_root(cls):
        """Return True if the root vertex does exist"""
        root = Collection.find("/")
        session = get_graph_session()
        query = gq_get_vertex_collection(root)
        rset = session.execute_graph("v_root = {};".format(query))
        
        try:
            root = next(rset.current_rows)
            return True
        except StopIteration:
            return False


    @classmethod
    def create(cls, name, container='/', metadata=None, username=None):
        """Create a new collection"""
        from indigo.models import Notification
        from indigo.models import Resource
        path = merge(container, name)
        # Check if parent collection exists
        parent = Collection.find(container)
        if parent is None:
            raise NoSuchCollectionError(container)
        resource = Resource.find(merge(container, name))
        if resource is not None:
            raise ResourceConflictError(container)
        collection = Collection.find(path)
        if collection is not None:
            raise CollectionConflictError(container)
        now = datetime.now()
        # If we try to create a tree entry with no metadata, cassandra-driver
        # will fail as it tries to delete a static column
        if metadata:
            metadata_cass = meta_cdmi_to_cassandra(metadata)
            coll_entry = TreeEntry.create(container=path,
                                          name='.',
                                          container_create_ts=now,
                                          container_modified_ts=now,
                                          container_metadata=metadata_cass)
        else:
            coll_entry = TreeEntry.create(container=path,
                                          name='.',
                                          container_create_ts=now,
                                          container_modified_ts=now)
        coll_entry.update(uuid=coll_entry.container_uuid)
        child_entry = TreeEntry.create(container=container,
                                       name=name + '/',
                                       uuid=coll_entry.container_uuid)
        
        new = Collection.find(path)
        
        add_user_edge = ""
        if username:
            user = User.find(username)
            if user:
                add_user_edge = """v_user = {}.next();
                                   v_user.addEdge('owns', v_new);
                                   """.format(gq_get_vertex_user(user))
        else:
            add_user_edge = ""
        session = get_graph_session()
        session.execute_graph("""v_parent = {}.next();
                                 v_new = {};
                                 v_parent.addEdge('son', v_new);
                                 {}
                                 """.format(gq_get_vertex_collection(parent),
                                            gq_add_vertex_collection(new),
                                            add_user_edge)
                             )
        if metadata:
            new.update_graph(metadata)

        state = new.mqtt_get_state()
        payload = new.mqtt_payload({}, state)
        Notification.create_collection(username, path, payload)
        # Index the collection
        new.index()
        return new


    @classmethod
    def create_graph_root(cls, root_entry):
        """Create the vertex for the root in the graph store"""
        session = get_graph_session()
        session.execute_graph(gq_add_vertex_collection(root_entry))


    @classmethod
    def create_root(cls):
        """Create the root container"""
        now = datetime.now()
        root_entry = TreeEntry.create(container='/',
                                      name='.',
                                      container_create_ts=now,
                                      container_modified_ts=now)
        root_entry.update(uuid=root_entry.container_uuid)
        root_entry.add_default_acl()
        
        root = cls(root_entry)
        cls.create_graph_root(root)
        
        return root


    @classmethod
    def delete_all(cls, path, username=None):
        """Delete recursively all sub-collections and all resources contained
        in a collection at 'path'"""
        from indigo.models import Resource
        parent = Collection.find(path)
        if not parent:
            return
        collections, resources = parent.get_child()
        collections = [Collection.find(merge(path, c)) for c in collections]
        resources = [Resource.find(merge(path, c)) for c in resources]
        for resource in resources:
            resource.delete(username)
        for collection in collections:
            Collection.delete_all(collection.path, username)
        parent.delete(username)


    @classmethod
    def find(cls, path):
        """Find a collection by path, initialise the collection with the
        appropriate row in the tree_entry table"""
        entries = TreeEntry.objects.filter(container=path, name=".")
        if not entries:
            return None
        else:
            return cls(entries.first())

    @classmethod
    def get_root(cls):
        """Return the root collection, Create it if it doesn't exist"""
        root = Collection.find("/")
        if not root:
            Collection.create_root()
        else:
            # Root exists in Cassandra, we check that the corresponding
            # vertex also exists in the graph
            if not Collection.check_graph_root():
                Collection.create_graph_root(root)
        return root


    def create_acl_list(self, read_access, write_access):
        """Create ACL in the tree entry table from two lists of groups id,
        existing ACL are replaced"""
        self.entry.create_container_acl_list(read_access, write_access)


    def create_acl_cdmi(self, cdmi_acl):
        """Create ACL in the tree entry table from ACL in the cdmi format (list
        of ACE dictionary), existing ACL are replaced"""
        self.entry.create_container_acl_cdmi(cdmi_acl)


    def delete(self, username=None):
        """Delete a collection and the associated row in the tree entry table"""
        from indigo.models import Notification
        if self.is_root:
            return
        cfg = get_config(None)
        session = connection.get_session()
        keyspace = cfg.get('KEYSPACE', 'indigo')
        session.set_keyspace(keyspace)
        query = SimpleStatement("""DELETE FROM tree_entry WHERE container=%s""")
        session.execute(query, (self.path,))
        # Get the row that describe the collection as a child of its parent
        child = TreeEntry.objects.filter(container=self.container,
                                           name=u"{}/".format(self.name)).first()
        if child:
            child.delete()
        
        session = get_graph_session()
        session.execute_graph("""v_coll = {}.drop();
                                 """.format(gq_get_vertex_collection(self))
                             )
        
        state = self.mqtt_get_state()
        payload = self.mqtt_payload(state, {})
        Notification.delete_collection(username, self.path, payload)
        self.reset()


    def get_acl(self):
        """Return a dictionary of acl based on the Collection schema"""
        return self.entry.container_acl


    def get_acl_list(self):
        """Return two list of groups id which have read and write access"""
        read_access = []
        write_access = []
        for gid, ace in self.entry.container_acl.items():
            oper = acemask_to_str(ace.acemask, False)
            if oper == "read":
                read_access.append(gid)
            elif oper == "write":
                write_access.append(gid)
            elif oper == "read/write":
                read_access.append(gid)
                write_access.append(gid)
            else:
                # Unknown combination
                pass
        return read_access, write_access


    def get_acl_metadata(self):
        """Return a dictionary of acl based on the Collection schema"""
        return serialize_acl_metadata(self)


    def get_authorized_actions(self, user):
        """"Get available actions for user according to a group"""
        # Check permission on the parent container if there's no action
        # defined at this level
        if not self.entry.container_acl:
            if self.is_root:
                return set([])
            else:
                parent_container = Collection.find(self.container)
                return parent_container.get_authorized_actions(user)
        actions = set([])
        for gid in user.groups + ["AUTHENTICATED@"]:
            if gid in self.entry.container_acl:
                ace = self.entry.container_acl[gid]
                level = acemask_to_str(ace.acemask, False)
                if level == "read":
                    actions.add("read")
                elif level == "write":
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
                elif level == "read/write":
                    actions.add("read")
                    actions.add("write")
                    actions.add("delete")
                    actions.add("edit")
        return actions


    def get_child(self):
        """Return two lists for child container and child dataobjects"""
        entries = TreeEntry.objects.filter(container=self.path)
        child_container = []
        child_dataobject = []
        for entry in list(entries):
            if entry.name == '.':
                continue
            elif entry.name.endswith('/'):
                child_container.append(entry.name[:-1])
            else:
                child_dataobject.append(entry.name)
        return (child_container, child_dataobject)

    def get_child_resource_count(self):
        child_container, child_dataobject = self.get_child()
        return len(child_dataobject)


    def get_cdmi_metadata(self):
        """Return a dictionary of metadata"""
        return meta_cassandra_to_cdmi(self.entry.container_metadata)


    def get_graph_metadata(self):
        """Return a dictionary of metadata stored in the graph"""
        session = get_graph_session()
        result = session.execute_graph(gq_get_metadata_collection(self))
        return { r['label']:r['value'] 
                 for r in result 
                 if r['label'] not in RESERVED_META
               }


    def get_list_metadata(self):
        """Transform metadata to a list of couples for web ui"""
        return metadata_to_list(self.entry.container_metadata)


    def get_metadata_key(self, key):
        """Return the value of a metadata"""
        return decode_meta(self.entry.container_metadata.get(key, ""))

    def index(self):
        from indigo.models import SearchIndex
        self.reset()
        SearchIndex.index(self, ['name', 'metadata'])

    def mqtt_get_state(self):
        """Get the collection state for the payload"""
        payload = dict()
        payload['uuid'] = self.uuid
        payload['container'] = self.container
        payload['name'] = self.name
        payload['create_ts'] = self.create_ts
        payload['modified_ts'] = self.entry.container_modified_ts
        payload['metadata'] = self.get_cdmi_metadata()
        return payload


    def mqtt_payload(self, pre_state, post_state):
        """Get a string version of the payload of the message"""
        payload = dict()
        payload['pre'] = pre_state
        payload['post'] = post_state
        return json.dumps(payload, default=datetime_serializer)

    def reset(self):
        from indigo.models import SearchIndex
        SearchIndex.reset(self.path)


    def to_dict(self, user=None):
        """Return a dictionary which describes a collection for the web ui"""
        data = {
            "id": self.uuid,
            "container": self.path,
            "name": self.name,
            "path": self.path,
            "created": self.create_ts,
            "metadata": self.get_list_metadata()
        }
        if user:
            data['can_read'] = self.user_can(user, "read")
            data['can_write'] = self.user_can(user, "write")
            data['can_edit'] = self.user_can(user, "edit")
            data['can_delete'] = self.user_can(user, "delete")
        return data


    def update(self, **kwargs):
        """Update a collection"""
        from indigo.models import Notification
        pre_state = self.mqtt_get_state()
        kwargs['container_modified_ts'] = datetime.now()
        if 'metadata' in kwargs:
            # Transform the metadata in cdmi format to the format stored in
            # Cassandra
            metadata = meta_cdmi_to_cassandra(kwargs['metadata'])
            self.update_graph(kwargs['metadata'])
            kwargs['container_metadata'] = metadata
            del kwargs['metadata']
        if 'username' in kwargs:
            username = kwargs['username']
            del kwargs['username']
        else:
            username = None
        self.entry.update(**kwargs)
        coll = Collection.find(self.path)
        post_state = coll.mqtt_get_state()
        payload = coll.mqtt_payload(pre_state, post_state)
        Notification.update_collection(username, coll.path, payload)
        coll.index()


    def update_acl_list(self, read_access, write_access):
        """Update ACL in the tree entry table from two lists of groups id,
        existing ACL are replaced"""
        self.entry.update_container_acl_list(read_access, write_access)


    def update_acl_cdmi(self, cdmi_acl):
        """Update ACL in the tree entry table from ACL in the cdmi format (list
        of ACE dictionary), existing ACL are replaced"""
        self.entry.update_container_acl_cdmi(cdmi_acl)


    def update_graph(self, metadata):
        """Get the new metadata and store them in the graph as properties"""
        session = get_graph_session()
        gq_coll = gq_get_vertex_collection(self)
        
        # old_meta stores the metadata present in the graph before updating
        old_meta = self.get_graph_metadata()
        # new_meta stores the set of final metadata we want
        new_meta = metadata
        
        old_meta_keys = set(old_meta.keys())
        new_meta_keys = set(new_meta.keys())
        
        # Delete metadata present in old but not in new
        for meta_to_delete in old_meta_keys.difference(new_meta_keys):
            session.execute_graph("""{}.properties('{}').drop()""".format(
                gq_coll,
                meta_to_delete))
        
        # Update metadata present in both old and new (check if value changed ?)
        for meta_to_update in old_meta_keys.intersection(new_meta_keys):
            session.execute_graph("""{}.property('{}', '{}')""".format(
                gq_coll,
                meta_to_update,
                new_meta[meta_to_update]))
        
        # Add metadata present in new but not in old
        for meta_to_add in new_meta_keys.difference(old_meta_keys):
            session.execute_graph("""{}.property('{}', '{}')""".format(
                gq_coll,
                meta_to_add,
                new_meta[meta_to_add]))


    def user_can(self, user, action):
        """
        User can perform the action if any of the user's group IDs
        appear in this list for 'action'_access in this object.
        """
        if user.administrator:
            # An administrator can do anything
            return True
        actions = self.get_authorized_actions(user)
        if action in actions:
            return True
        return False


