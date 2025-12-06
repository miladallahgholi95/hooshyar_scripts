NAME = "hokmrani_dade_paragraph_index"
MAPPING = {
      "properties": {
        "content": {
          "type": "text",
          "term_vector": "with_positions_offsets",
          "analyzer": "persian_custom_analyzer"
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
        "document_last_status": {
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
            "dates": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "locations": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "money": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "name": {
              "type": "keyword"
            },
            "organizations": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "percents": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "persons": {
              "properties": {
                "end": {
                  "type": "long"
                },
                "start": {
                  "type": "long"
                },
                "word": {
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
            "value": {
              "type": "nested"
            }
          }
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
        "main_subject": {
          "type": "keyword"
        },
        "paragraph_number": {
          "type": "integer"
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
        }
      }
    }