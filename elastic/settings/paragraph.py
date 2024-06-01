SETTING = {
        "analysis": {
            "filter": {"persian_synonyms": {"type": "synonym_graph", "synonyms_path": "synonyms.txt"}},
            "char_filter": {
                "zero_width_spaces": {"type": "mapping", "mappings_path": "char_filter/zero_width_spaces_filter.txt"}
            },
            "analyzer": {
                "persian_custom_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["zero_width_spaces"],
                    "filter": [
                        "lowercase",
                        "decimal_digit",
                        "parsi_normalizer",
                    ],
                },
                "persian_synonym_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "char_filter": ["zero_width_spaces"],
                    "filter": [
                        "lowercase",
                        "decimal_digit",
                        "parsi_normalizer",
                        "persian_synonyms",
                    ],
                },
            },
        },
        "number_of_shards": 5,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
        "index.max_result_window": "5000",
    }