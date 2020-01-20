# -*- coding: utf-8 -*-
from openalea.core.path import path
from openalea.core import settings
import os
from os.path import expanduser


PROVENANCE_PATH = path(settings.get_openalea_home_dir()) / 'provenance'

# infos about provenance db
REMOTE_PROV = False
# Mongo
MONGO_ADDR='127.0.0.1'
MONGO_PORT = 27017

# remote db
MONGO_SSH_IP='127.0.0.1'
home = expanduser("~")
SSH_PKEY = os.path.join(home, ".ssh", "id_rsa")
SSH_USERNAME="ubuntu"