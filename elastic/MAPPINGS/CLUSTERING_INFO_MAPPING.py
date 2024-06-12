NAME = "hooshyar3_clustering_info_index_v33i"
MAPPING = {
      "properties": {
        "algorithm": {
          "type": "keyword"
        },
        "cluster_count": {
          "type": "short"
        },
        "cluster_keywords": {
          "properties": {
            "keyword": {
              "type": "keyword"
            },
            "score": {
              "type": "float"
            }
          }
        },
        "cluster_number": {
          "type": "short"
        },
        "entropy": {
          "type": "float"
        },
        "feature_type": {
          "type": "keyword"
        },
        "main_subject": {
          "type": "keyword"
        },
        "paragraphs_count": {
          "type": "integer"
        },
        "source_id": {
          "type": "short"
        },
        "source_name": {
          "type": "keyword"
        },
        "subjects": {
          "properties": {
            "name": {
              "type": "keyword"
            },
            "score": {
              "type": "float"
            }
          }
        },
        "vector_type": {
          "type": "keyword"
        }
      }
    }