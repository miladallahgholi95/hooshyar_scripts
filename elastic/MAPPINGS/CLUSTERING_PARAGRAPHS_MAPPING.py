NAME = "hooshyar3_clustering_paragraphs_index_v33i"
MAPPING = {
      "properties": {
        "algorithm": {
          "type": "keyword"
        },
        "clusters": {
          "type": "nested",
          "properties": {
            "cluster_count": {
              "type": "short"
            },
            "cluster_number": {
              "type": "short"
            }
          }
        },
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
        "document_name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "term_vector": "with_positions_offsets"
        },
        "feature_type": {
          "type": "keyword"
        },
        "main_subject": {
          "type": "keyword"
        },
        "paragraph_id": {
          "type": "keyword"
        },
        "source_id": {
          "type": "short"
        },
        "source_name": {
          "type": "keyword"
        },
        "vector_type": {
          "type": "keyword"
        }
      }
    }