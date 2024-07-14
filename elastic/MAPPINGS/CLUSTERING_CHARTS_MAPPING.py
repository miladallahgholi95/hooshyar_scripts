NAME = "hooshyar_clustering_charts_index"
MAPPING = {
      "properties": {
        "algorithm": {
          "type": "keyword"
        },
        "cluster_count": {
          "type": "short"
        },
        "cosine_distance_chart": {
          "properties": {
            "destination": {
              "type": "short"
            },
            "distance": {
              "type": "float"
            },
            "source": {
              "type": "short"
            }
          }
        },
        "euclidean_distance_chart": {
          "properties": {
            "destination": {
              "type": "short"
            },
            "distance": {
              "type": "float"
            },
            "source": {
              "type": "short"
            }
          }
        },
        "feature_type": {
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