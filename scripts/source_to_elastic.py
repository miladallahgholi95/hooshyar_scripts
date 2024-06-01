
from elastic.connection import *
from elastic.MAPPINGS import country
from elastic.SETTINGS import DOCUMENT_SETTING


def apply():
    if not ES_CLIENT.indices.exists(index=country.NAME):
        new_index = IndexObjectWithId(country.NAME, DOCUMENT_INDEX_SETTING, SOURCE_INDEX_MAPPING)
        new_index.create()

    data = {
            "name": SOURCE_NAME,
            "language": SOURCE_LANGUAGE
        }

    client.index(
        index=SOURCE_INDEX_NAME,
        id=SOURCE_ID,
        document=data,
        refresh=True)
