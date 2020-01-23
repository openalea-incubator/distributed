# -*- coding: utf-8 -*-
from openalea.core.path import path
from openalea.core import settings
import os
from os.path import expanduser

REMOTE_PROV = False

# Files
PROVENANCE_PATH = path(settings.get_openalea_home_dir()) / 'provenance'

# Mongo
MONGO_ADDR='127.0.0.1'
MONGO_PORT = 27017

# remote db
MONGO_SSH_IP='127.0.0.1'
home = expanduser("~")
SSH_PKEY = os.path.join(home, ".ssh", "id_rsa")
SSH_USERNAME="ubuntu"


# cassandra
CASSANDRA_SSH_IP = "134.158.247.62"
CASSANDRA_ADDR = "localhost" 
CASSANDRA_PORT = 9042