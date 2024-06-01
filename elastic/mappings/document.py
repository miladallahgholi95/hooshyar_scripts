NAME = "hooshyar3_document_index_v3"
MAPPING = {
      "properties": {
        "actors": {
          "type": "nested",
          "properties": {
            "frequency": {
              "type": "short"
            },
            "id": {
              "type": "short"
            },
            "name": {
              "type": "keyword"
            }
          }
        },
        "affect_detail": {
          "type": "nested",
          "properties": {
            "clause": {
              "type": "text"
            },
            "document_date": {
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
              "term_vector": "with_positions_offsets",
              "analyzer": "persian_custom_analyzer"
            },
            "paragraph_text": {
              "type": "text",
              "term_vector": "with_positions_offsets",
              "analyzer": "persian_custom_analyzer"
            },
            "status": {
              "type": "keyword"
            },
            "status_type": {
              "type": "keyword"
            }
          }
        },
        "affected_by": {
          "type": "keyword"
        },
        "affected_graph_data": {
          "properties": {
            "edges_data": {
              "type": "flattened"
            },
            "nodes_data": {
              "type": "flattened"
            }
          }
        },
        "affecting": {
          "type": "keyword"
        },
        "category": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "analyzer": "persian_custom_analyzer"
        },
        "content": {
          "type": "text",
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer"
        },
        "datetime": {
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
        "download_datetime": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
        },
        "extra_data": {
          "type": "nested",
          "properties": {
            "name": {
              "type": "keyword"
            },
            "value": {
              "type": "keyword"
            }
          }
        },
        "file_path": {
          "type": "keyword"
        },
        "keyword_main_subject": {
          "type": "keyword"
        },
        "keyword_subjects": {
          "type": "nested",
          "properties": {
            "name": {
              "type": "keyword"
            },
            "value": {
              "type": "float"
            }
          }
        },
        "keyword_subjects_words": {
          "type": "nested",
          "properties": {
            "name": {
              "type": "keyword"
            },
            "value": {
              "type": "keyword"
            }
          }
        },
        "last_status": {
          "type": "keyword"
        },
        "level": {
          "type": "keyword"
        },
        "main_subject": {
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
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer"
        },
        "regulation_related": {
          "type": "keyword"
        },
        "related_graph_data": {
          "properties": {
            "edges_data": {
              "type": "flattened"
            },
            "nodes_data": {
              "type": "flattened"
            }
          }
        },
        "revoked_type_name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "rules_related": {
          "type": "keyword"
        },
        "source_id": {
          "type": "short"
        },
        "source_name": {
          "type": "keyword"
        },
        "source_url": {
          "type": "keyword"
        },
        "subjects": {
          "type": "nested",
          "properties": {
            "name": {
              "type": "keyword"
            },
            "value": {
              "type": "float"
            }
          }
        },
        "tika_meta_data": {
          "properties": {
            "character_count": {
              "type": "integer"
            },
            "character_count_with_spaces": {
              "type": "integer"
            },
            "content_length": {
              "type": "integer"
            },
            "create_datetime": {
              "type": "date",
              "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
            },
            "format": {
              "type": "keyword"
            },
            "modified_datetime": {
              "type": "date",
              "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
            },
            "number_of_pages": {
              "type": "integer"
            },
            "paragraph_count": {
              "type": "short"
            },
            "word_count": {
              "type": "integer"
            }
          }
        },
        "type": {
          "type": "keyword"
        }
      }
    }