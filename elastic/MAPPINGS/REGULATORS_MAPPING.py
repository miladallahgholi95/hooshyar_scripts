NAME = "hooshyar_regulators_index"
MAPPING = {
      "properties": {
        "area": {
          "type": "keyword"
        },
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "analyzer": "persian_custom_analyzer"
        },
        "related_actor": {
          "type": "keyword"
        }
      }
    }