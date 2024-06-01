from input_configs import *
from elastic.connection import *
from elastic.MAPPINGS import SOURCE_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING


def apply():
    if not ES_CLIENT.indices.exists(index=SOURCE_MAPPING.NAME):
        new_index = IndexObjectWithId(SOURCE_MAPPING.NAME, DOCUMENT_SETTING.SETTING, SOURCE_MAPPING.MAPPING)
        new_index.create()
    data = {
            "name": SOURCE_NAME,
            "language": SOURCE_LANGUAGE
        }
    ES_CLIENT.index(
        index=SOURCE_MAPPING.NAME,
        id=SOURCE_ID,
        document=data,
        refresh=True)
