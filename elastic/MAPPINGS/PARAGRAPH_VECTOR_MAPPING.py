NAME = "hooshyar3_paragraphs_vector_index_v33i"
MAPPING = {
      "properties": {
        "content": {
          "type": "text",
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer"
        },
        "document_datetime": {
          "properties": {
            "day": {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "number": {
                  "type": "short"
                }
              }
            },
            "hour": {
              "type": "short"
            },
            "minute": {
              "type": "short"
            },
            "month": {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "number": {
                  "type": "short"
                }
              }
            },
            "year": {
              "type": "short"
            }
          }
        },
        "document_id": {
          "type": "keyword"
        },
        "document_level": {
          "type": "keyword"
        },
        "document_name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer"
        },
        "document_subject": {
          "type": "keyword"
        },
        "document_type": {
          "type": "keyword"
        },
        "paragraph_number": {
          "type": "integer"
        },
        "source_id": {
          "type": "short"
        },
        "source_name": {
          "type": "keyword"
        },
        "vector_hooshyar": {
          "type": "dense_vector",
          "dims": 768
        }
      }
    }