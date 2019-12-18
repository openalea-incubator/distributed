from openalea.core.path import path
from openalea.core import settings

PROVENANCE_PATH = path(settings.get_openalea_home_dir()) / 'provenance'
TMP_PATH = path(settings.get_openalea_home_dir()) / "execution_data"
CACHE_PATH = path(settings.get_openalea_home_dir()) / "cached_data"