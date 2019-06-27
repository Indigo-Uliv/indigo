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

from indigo.models.activity import Activity

class ActivityTest(unittest.TestCase):

    def test_actvity_ordering(self):
        # Make sure the order we create them (oldest first) is
        # also the way we get the activities back from the DB.
        new_activities = [Activity.new("Random activity") for x in xrange(20)]
        activities = Activity.recent(10)

        # We want the most recent 10 from the new items we created ...
        new_activity_ids = [a.when for a in new_activities[10:]]
        new_activity_ids.reverse()

        activity_ids = [a.when for a in activities]


        assert activity_ids == new_activity_ids
