NAME = "hooshyar3_references_index_v33i"
MAPPING = {
      "properties": {
        "edge_data": {
          "type": "flattened"
        },
        "node_data": {
          "type": "flattened"
        },
        "source_document_category": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "analyzer": "persian_custom_analyzer"
        },
        "source_document_datetime": {
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
        "source_document_id": {
          "type": "keyword"
        },
        "source_document_level": {
          "type": "keyword"
        },
        "source_document_main_subject": {
          "type": "keyword"
        },
        "source_document_name": {
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
        "source_document_source_id": {
          "type": "short"
        },
        "source_document_source_name": {
          "type": "keyword"
        },
        "source_document_type": {
          "type": "keyword"
        },
        "target_document_category": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "analyzer": "persian_custom_analyzer"
        },
        "target_document_datetime": {
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
        "target_document_id": {
          "type": "keyword"
        },
        "target_document_level": {
          "type": "keyword"
        },
        "target_document_main_subject": {
          "type": "keyword"
        },
        "target_document_name": {
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
        "target_document_source_id": {
          "type": "short"
        },
        "target_document_source_name": {
          "type": "keyword"
        },
        "target_document_type": {
          "type": "keyword"
        },
        "text_indices_list": {
          "type": "nested",
          "properties": {
            "indices": {
              "type": "text"
            },
            "paragraph_id": {
              "type": "keyword"
            }
          }
        }
      }
    }