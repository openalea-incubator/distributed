from openalea.core.path import path
from openalea.core import settings
import os
from os.path import expanduser

# PATHS
# files infos
PROVENANCE_PATH = path(settings.get_openalea_home_dir()) / 'provenance'
TMP_PATH = path(settings.get_openalea_home_dir()) / "execution_data"
CACHE_PATH = path(settings.get_openalea_home_dir()) / "cached_data"

# infos about provenance db
REMOTE = True
# Mongo
MONGO_ADDR='127.0.0.1'
MONGO_PORT = 27017

# cassandra
CASSANDRA_SSH_IP = "134.158.247.62"
CASSANDRA_PORT = 9042


# SSH
PROVDB_SSH_ADDR = ""
home = expanduser("~")
SSH_PKEY = os.path.join(home, ".ssh", "id_rsa")
SSH_USERNAME="ubuntu"
# REMOTE_DB_ADDR=('127.0.0.1', 27017)

