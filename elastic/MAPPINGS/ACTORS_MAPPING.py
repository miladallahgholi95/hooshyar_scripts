NAME = "hokmrani_dade_actors_index"
MAPPING = {
      "properties": {
        "area": {
          "type": "keyword"
        },
        "arima_prediction_data": {
          "type": "nested",
          "properties": {
            "RMSE": {
              "type": "float"
            },
            "best_parameters": {
              "properties": {
                "d": {
                  "type": "short"
                },
                "p": {
                  "type": "short"
                },
                "q": {
                  "type": "short"
                }
              }
            },
            "p_value_first": {
              "type": "float"
            },
            "p_value_last": {
              "type": "float"
            },
            "prediction_data": {
              "properties": {
                "count": {
                  "type": "short"
                },
                "year": {
                  "type": "short"
                }
              }
            },
            "role_name": {
              "type": "keyword"
            },
            "source_id": {
              "type": "short"
            }
          }
        },
        "category": {
          "type": "keyword"
        },
        "forms": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "analyzer": "persian_custom_analyzer"
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
        "source_correlation_data": {
          "type": "nested",
          "properties": {
            "correlation_data": {
              "properties": {
                "actor_id": {
                  "type": "short"
                },
                "actor_name": {
                  "type": "keyword"
                },
                "correlation_value": {
                  "type": "float"
                }
              }
            },
            "role_name": {
              "type": "keyword"
            },
            "source_id": {
              "type": "short"
            }
          }
        },
        "source_series_data": {
          "type": "nested",
          "properties": {
            "role_name": {
              "type": "keyword"
            },
            "series_data": {
              "properties": {
                "count": {
                  "type": "short"
                },
                "year": {
                  "type": "short"
                }
              }
            },
            "source_id": {
              "type": "short"
            }
          }
        }
      }
    }