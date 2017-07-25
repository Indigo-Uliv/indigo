from indigo.models import initialise, sync
from dse.cqlengine.management import drop_keyspace

TEST_KEYSPACE="indigo_test"

def setup_package():
    initialise(keyspace=TEST_KEYSPACE, strategy="SimpleStrategy", repl_factor=1)
    sync()

def teardown_package():
    drop_keyspace(TEST_KEYSPACE)