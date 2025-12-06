NAME = "hokmrani_dade_paragraph_actor_index"
MAPPING = {
      "properties": {
        "actors": {
          "type": "nested",
          "properties": {
            "current_form": {
              "type": "keyword"
            },
            "detection_type": {
              "type": "keyword"
            },
            "general_definition_keyword": {
              "type": "keyword"
            },
            "general_definition_text": {
              "type": "text",
              "analyzer": "persian_custom_analyzer"
            },
            "id": {
              "type": "short"
            },
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "ref_paragraph_text": {
              "type": "text",
              "analyzer": "persian_custom_analyzer"
            },
            "role": {
              "type": "keyword"
            }
          }
        },
        "content": {
          "type": "text",
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer",
          "fielddata": True
        },
        "document_category": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
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
        "document_keyword_main_subject": {
          "type": "keyword"
        },
        "document_level": {
          "type": "keyword"
        },
        "document_main_subject": {
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
        "document_source_id": {
          "type": "short"
        },
        "document_source_name": {
          "type": "keyword"
        },
        "document_type": {
          "type": "keyword"
        },
        "entities": {
          "type": "nested",
          "properties": {
            "name": {
              "type": "keyword"
            },
            "value": {
              "type": "nested"
            }
          }
        },
        "keyword_main_subject": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "keyword_subjects": {
          "properties": {
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "value": {
              "type": "float"
            }
          }
        },
        "keyword_subjects_words": {
          "properties": {
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "value": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        },
        "main_subject": {
          "type": "keyword"
        },
        "paragraph_number": {
          "type": "integer"
        },
        "regulators": {
          "type": "nested",
          "properties": {
            "id": {
              "type": "short"
            },
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "tool_name": {
              "type": "keyword"
            }
          }
        },
        "sentiment": {
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
        "supervisors": {
          "type": "nested",
          "properties": {
            "source_actor_form": {
              "type": "keyword"
            },
            "source_actor_id": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "source_actor_name": {
              "type": "keyword"
            },
            "supervisor_actor_form": {
              "type": "keyword"
            },
            "supervisor_actor_id": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "supervisor_actor_name": {
              "type": "keyword"
            }
          }
        }
      }
    }