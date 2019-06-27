"""Common class for unittest - Project RADON version

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

import unittest

from indigo.models import Node
from indigo.models.errors import NodeConflictError

from nose.tools import raises

class NodeTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_create(self):
        Node.create(name="test", address="127.0.0.1")
        node = Node.find("test")
        assert node.name == "test"
        assert node.address == '127.0.0.1'
        assert node.status == 'UP'

    @raises(NodeConflictError)
    def test_create_fail(self):
        Node.create(name="test_fail", address="127.0.0.1")
        Node.create(name="test_fail", address="127.0.0.1")

    def test_setstatus(self):
        Node.create(name="status_test", address="127.0.0.2")
        node = Node.find("status_test")
        assert node.status == "UP"

        node.status_down()
        node = Node.find("status_test")
        assert node.status == "DOWN"
