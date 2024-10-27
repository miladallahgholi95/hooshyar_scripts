from sklearn.preprocessing import MaxAbsScaler
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
from hazm import *
from sklearn import cluster
from collections import Counter
from sklearn.metrics.pairwise import euclidean_distances, cosine_similarity
import numpy as np
from gensim.corpora import Dictionary
from gensim.models.ldamodel import LdaModel
from elastic.connection import ESIndex, ES_CLIENT, SEARCH_WINDOW_SIZE
from input_configs import *
from elastic.MAPPINGS import PARAGRAPH_MAPPING, CLUSTERING_PARAGRAPHS_MAPPING, CLUSTERING_INFO_MAPPING, CLUSTERING_CHARTS_MAPPING
from elastic.SETTINGS import DOCUMENT_SETTING

CURRENT_VECTOR_ID = 1


def local_processing(text):
    # normalizer = Normalizer()
    # text = normalizer.normalize(text)
    ignoreList = ["!", "@", "$", "%", "^", "&", "*", "(", ")", "_", "+", "-", "/", "*", "'", "،", "؛", ",",
                  "{", "}", '\xad', '­', "[", "]", "«", "»", "<", ">", ".", "?", "؟", "\n", "\t", '"',
                  '۱', '۲', '۳', '۴', '۵', '۶', '۷', '۸', '۹', '۰', "٫", ".",
                  "\u200C", "\u200B", "\u200E", "\u200F", "\u001F", "\u00AC",
                  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    for item in ignoreList:
        text = text.replace(item, " ")
    while "  " in text:
        text = text.replace("  ", " ")
    arabic_char = {"آ": "ا", "أ": "ا", "إ": "ا", "ي": "ی", "ئ": "ی", "ة": "ه", "ۀ": "ه", "ك": "ک", "َ": "", "ُ": "",
                   "ِ": ""}
    for key, value in arabic_char.items():
        text = text.replace(key, value)
    return text


def preprocessing(text):
    text = local_processing(text)
    text = [word for word in text.split(" ") if word != ""]
    stopword_list = open("./other_files/all_stopwords.txt", encoding="utf8").read().split("\n")
    stopword_list = list(set(stopword_list))
    text = [word for word in text if word not in stopword_list and len(word) >= 2]
    return text


def calculate_entropy(data):
    unique_values, counts = np.unique(data, return_counts=True)
    probabilities = counts / len(data)
    entropy = -np.sum(probabilities * np.log2(probabilities))
    return entropy


def create_heatmap_data(distance_array):
    chart_data = []
    i = 0
    for row in distance_array:
        i += 1
        j = 0
        x_value = str(i)
        for col in row:
            j += 1
            y_value = str(j)
            block = {"source": x_value, "destination": y_value, "distance": round(col, 3)}
            chart_data.append(block)
    return chart_data


def tf_idf_embedding(corpus, ngram_type):
    min_df = 0.01
    max_df = 0.35
    if ngram_type == (2, 2):
        min_df = 0.005

    vectorizer = TfidfVectorizer(max_df=max_df, min_df=min_df, ngram_range=ngram_type)
    corpus_transform = vectorizer.fit_transform(corpus)
    scaler = MaxAbsScaler()
    corpus_transform = scaler.fit_transform(corpus_transform)

    final_result_dataframe = pd.DataFrame(data=corpus_transform.toarray(), columns=vectorizer.get_feature_names_out())
    features = vectorizer.get_feature_names_out()

    return final_result_dataframe, features


def get_cluster_best_feature(corpus_feature_dataframe, clustering_model, n_features, features):
    corpus_feature_array = corpus_feature_dataframe.to_numpy()
    prediction = clustering_model.predict(corpus_feature_dataframe)
    labels = np.unique(prediction)
    best_features_dict = {}
    for label in labels:
        id_temp = np.where(prediction == label)
        x_means = np.mean(corpus_feature_array[id_temp], axis=0)
        sorted_means = np.argsort(x_means)[::-1][:n_features]
        best_features = [{"keyword": features[i], "score": x_means[i]} for i in sorted_means]
        best_features_dict[label] = best_features
    return best_features_dict


def kmeans_clustering(corpus, para_id_list, paragraph_data_dict, config_dict, source_id):
    feature_type = config_dict["feature_type"]
    corpus_feature_dataframe, features = config_dict["vector_extractor_function"](corpus, feature_type)

    min_k, max_k, step = config_dict["cluster_count_min"], config_dict["cluster_count_max"], config_dict[
        "cluster_count_step"]

    clusters_info_list = []
    clusters_chart_list = []
    for k in range(min_k, max_k + 1, step):
        print(f"clustering k = {k}")

        kmeans_model = cluster.MiniBatchKMeans(n_clusters=k
                                               , init='k-means++'
                                               , n_init=10
                                               , tol=0.0001
                                               , random_state=1
                                               , max_no_improvement=200
                                               , batch_size=config_dict['batch_size']
                                               , max_iter=1000).fit(corpus_feature_dataframe)
        labels = kmeans_model.labels_

        clusters_info_dict = {
            i: {
                "source_id": source_id,
                "source_name": "",
                "algorithm": config_dict["algorithm"],
                "vector_type": config_dict["vector_type"],
                "feature_type": str(config_dict["feature_type"]),
                "cluster_count": k,
                "cluster_number": i,
                "cluster_keywords": [],
                "paragraphs_count": 0,
                "entropy": 0,
                "main_subject": "",
                "subjects": []
            } for i in range(k)
        }
        source_name = ""
        best_features_dict = get_cluster_best_feature(corpus_feature_dataframe, kmeans_model, 20, features)
        for i in range(len(para_id_list)):
            para_id = para_id_list[i]
            main_subject = paragraph_data_dict[para_id]["main_subject"]

            cluster_number = labels[i]

            topic_dict = {
                "cluster_count": k,
                "cluster_number": labels[i]
            }

            paragraph_data_dict[para_id]['clusters'].append(topic_dict)
            paragraph_data_dict[para_id]["algorithm"] = config_dict["algorithm"]
            paragraph_data_dict[para_id]["vector_type"] = config_dict["vector_type"]
            paragraph_data_dict[para_id]["feature_type"] = str(config_dict["feature_type"])

            clusters_info_dict[cluster_number]["paragraphs_count"] += 1
            clusters_info_dict[cluster_number]["subjects"].append(main_subject)
            clusters_info_dict[cluster_number]["source_name"] = paragraph_data_dict[para_id]["source_name"]
            clusters_info_dict[cluster_number]["cluster_keywords"] = best_features_dict[cluster_number]
            source_name = paragraph_data_dict[para_id]["source_name"]
        for key, value in clusters_info_dict.items():
            subject_dict = Counter(value["subjects"])
            clusters_info_dict[key]["entropy"] = calculate_entropy(value["subjects"])
            clusters_info_dict[key]["subjects"] = [{"name": subject, "score": score} for subject, score in
                                                   dict(subject_dict).items()]
            clusters_info_dict[key]["main_subject"] = str(max(subject_dict, key=subject_dict.get))
            clusters_info_list.append(clusters_info_dict[key])

        euclidean_distance_array = euclidean_distances(kmeans_model.cluster_centers_)
        cosine_distance_array = cosine_similarity(kmeans_model.cluster_centers_)
        euclidean_distance_chart_data = create_heatmap_data(euclidean_distance_array)
        cosine_distance_chart_data = create_heatmap_data(cosine_distance_array)

        chart_data = {
            "source_id": source_id,
            "source_name": source_name,
            "algorithm": config_dict["algorithm"],
            "vector_type": config_dict["vector_type"],
            "feature_type": str(config_dict["feature_type"]),
            "cluster_count": k,
            "euclidean_distance_chart": euclidean_distance_chart_data,
            "cosine_distance_chart": cosine_distance_chart_data,
        }

        clusters_chart_list.append(chart_data)

    clusters_paragraphs_list = []
    for para_id, data in paragraph_data_dict.items():
        data["_id"] = str(data["source_id"]) + "_" + \
                      config_dict["algorithm"] + "_" + \
                      config_dict["vector_type"] + "_" + \
                      para_id
        clusters_paragraphs_list.append(data)

    return clusters_info_list, clusters_chart_list, clusters_paragraphs_list


def lda_clustering(corpus, para_id_list, paragraph_data_dict, config_dict, source_id):
    min_k, max_k, step = config_dict["cluster_count_min"], config_dict["cluster_count_max"], config_dict[
        "cluster_count_step"]

    clusters_info_list = []
    clusters_chart_list = []

    processed_texts = [preprocessing(text) for text in corpus]
    dictionary = Dictionary(processed_texts)
    dictionary.filter_extremes(no_below=5, no_above=0.5)
    corpus_bow = [dictionary.doc2bow(text) for text in processed_texts]

    for k in range(min_k, max_k + 1, step):
        print(f"clustering k = {k}")

        lda_model = LdaModel(corpus_bow, num_topics=k, id2word=dictionary)

        clusters_info_dict = {
            i: {
                "source_id": source_id,
                "source_name": "",
                "algorithm": config_dict["algorithm"],
                "vector_type": config_dict["vector_type"],
                "feature_type": str(config_dict["feature_type"]),
                "cluster_count": k,
                "cluster_number": i,
                "cluster_keywords": [],
                "paragraphs_count": 0,
                "entropy": 0,
                "main_subject": "",
                "subjects": []
            } for i in range(k)
        }
        source_name = ""
        for i in range(len(para_id_list)):
            para_id = para_id_list[i]
            main_subject = paragraph_data_dict[para_id]["main_subject"]
            cluster_number = max(lda_model.get_document_topics(corpus_bow[i]), key=lambda x: x[1])[0]
            topic_dict = {
                "cluster_count": k,
                "cluster_number": cluster_number
            }

            cluster_keywords = [{"keyword": keyword, "score": score} for keyword, score in
                                lda_model.show_topic(cluster_number, topn=20)]

            paragraph_data_dict[para_id]['clusters'].append(topic_dict)
            paragraph_data_dict[para_id]["algorithm"] = config_dict["algorithm"]
            paragraph_data_dict[para_id]["vector_type"] = config_dict["vector_type"]
            paragraph_data_dict[para_id]["feature_type"] = str(config_dict["feature_type"])

            clusters_info_dict[cluster_number]["paragraphs_count"] += 1
            clusters_info_dict[cluster_number]["subjects"].append(main_subject)
            clusters_info_dict[cluster_number]["source_name"] = paragraph_data_dict[para_id]["source_name"]
            clusters_info_dict[cluster_number]["cluster_keywords"] = cluster_keywords
            source_name = paragraph_data_dict[para_id]["source_name"]

        for key, value in clusters_info_dict.items():
            subject_dict = Counter(value["subjects"])
            clusters_info_dict[key]["entropy"] = calculate_entropy(value["subjects"])
            clusters_info_dict[key]["subjects"] = [{"name": subject, "score": score} for subject, score in
                                                   dict(subject_dict).items()]
            clusters_info_dict[key]["main_subject"] = str(max(subject_dict, key=subject_dict.get))
            clusters_info_list.append(clusters_info_dict[key])

        euclidean_distance_array = euclidean_distances(lda_model.get_topics())
        cosine_distance_array = cosine_similarity(lda_model.get_topics())
        euclidean_distance_chart_data = create_heatmap_data(euclidean_distance_array)
        cosine_distance_chart_data = create_heatmap_data(cosine_distance_array)

        chart_data = {
            "source_id": source_id,
            "source_name": source_name,
            "algorithm": config_dict["algorithm"],
            "vector_type": config_dict["vector_type"],
            "feature_type": str(config_dict["feature_type"]),
            "cluster_count": k,
            "euclidean_distance_chart": euclidean_distance_chart_data,
            "cosine_distance_chart": cosine_distance_chart_data,
        }

        clusters_chart_list.append(chart_data)

    clusters_paragraphs_list = []
    for para_id, data in paragraph_data_dict.items():
        data["_id"] = str(data["source_id"]) + "_" + \
                      config_dict["algorithm"] + "_" + \
                      config_dict["vector_type"] + "_" + \
                      para_id
        clusters_paragraphs_list.append(data)

    return clusters_info_list, clusters_chart_list, clusters_paragraphs_list


def create_corpus(source_id, filter_content_length, subject_field_name):
    res_query = {
        "bool": {
            "filter": [
                {
                    "term": {
                        "document_source_id": source_id
                    }
                }
                , {
                    "term": {
                        "document_type": "قانون"
                    }
                }
            ]
        }
    }

    print("get paragraph count ...")
    index_paragraph_size = ES_CLIENT.count(body={
        "query": res_query
    }, index=PARAGRAPH_MAPPING.NAME)['count']
    if index_paragraph_size > SEARCH_WINDOW_SIZE:
        ES_CLIENT.indices.put_settings(index=PARAGRAPH_MAPPING.NAME,
                                       body={"index": {
                                           "max_result_window": index_paragraph_size
                                       }})
    print("get paragraph data ...")
    response = ES_CLIENT.search(index=PARAGRAPH_MAPPING.NAME,
                                request_timeout=120,
                                query=res_query,
                                size=index_paragraph_size
                                )

    if index_paragraph_size > SEARCH_WINDOW_SIZE:
        ES_CLIENT.indices.put_settings(index=PARAGRAPH_MAPPING.NAME,
                                       body={"index": {
                                           "max_result_window": SEARCH_WINDOW_SIZE
                                       }})

    paragraphs_list = response['hits']['hits']
    print(f"all paragraph count is: {len(paragraphs_list)}")
    corpus = []
    para_id_list = []
    paragraph_data_dict = {}
    i = 1
    for para in paragraphs_list:
        para_text = para['_source']['content']
        if len(para_text) > filter_content_length:
            para_token_list = preprocessing(para_text)
            new_para_text = " ".join(para_token_list)
            corpus.append(new_para_text)
            para_id_list.append(para['_id'])
            paragraph_data_dict[para['_id']] = {
                "paragraph_id": para['_id'],
                "source_id": para['_source']["document_source_id"],
                "source_name": para['_source']["document_source_name"],
                "document_id": para['_source']["document_id"],
                "document_name": para['_source']["document_name"],
                "document_datetime": para['_source']["document_datetime"],
                "main_subject": para['_source'][subject_field_name] if para['_source'][subject_field_name] not in ["",
                                                                                                                   None] else 'نامشخص',
                "content": para_text,
                "clusters": []
            }

        print(f"create corpus paragraphs: {i}/{len(paragraphs_list)}")
        i += 1

    print(f"valid paragraphs count is: {len(para_id_list)}")
    return corpus, para_id_list, paragraph_data_dict


class ClusterInfoIndex(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, records):
        for record in records:
            new_doc = {
                "source_id": record["source_id"],
                "source_name": record["source_name"],
                "algorithm": record["algorithm"],
                "vector_type": record["vector_type"],
                "feature_type": record["feature_type"],
                "cluster_count": record["cluster_count"],
                "cluster_number": record["cluster_number"],
                "cluster_keywords": record["cluster_keywords"],
                "paragraphs_count": record["paragraphs_count"],
                "entropy": record["entropy"],
                "main_subject": record["main_subject"],
                "subjects": record["subjects"]
            }

            new_document = {
                "_index": self.name,
                "_source": new_doc,
            }
            yield new_document


class ClusterChartIndex(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, records):
        for record in records:
            new_doc = {
                "source_id": record["source_id"],
                "source_name": record["source_name"],
                "algorithm": record["algorithm"],
                "vector_type": record["vector_type"],
                "feature_type": record["feature_type"],
                "cluster_count": record["cluster_count"],
                "euclidean_distance_chart": record["euclidean_distance_chart"],
                "cosine_distance_chart": record["cosine_distance_chart"]
            }

            new_document = {
                "_index": self.name,
                "_source": new_doc,
            }
            yield new_document


class ClusterParagraphsIndex(ESIndex):
    def __init__(self, name, settings, mappings):
        super().__init__(name, settings, mappings)

    def generate_docs(self, records):
        for record in records:
            new_doc = {
                "paragraph_id": record["paragraph_id"],
                "source_id": record["source_id"],
                "source_name": record["source_name"],
                "document_id": record["document_id"],
                "document_name": record["document_name"],
                "document_datetime": record["document_datetime"],
                "main_subject": record["main_subject"],
                "content": record["content"],
                "algorithm": record["algorithm"],
                "vector_type": record["vector_type"],
                "feature_type": record["feature_type"],
                "clusters": record["clusters"]
            }

            new_document = {
                "_index": self.name,
                "_source": new_doc,
                "_id": record["_id"]
            }
            yield new_document


def ingest_data_to_elastic(source_id, clusters_info_list, clusters_chart_list, clusters_paragraphs_list):
    info_index = ClusterInfoIndex(CLUSTERING_INFO_MAPPING.NAME,
                                  DOCUMENT_SETTING.SETTING,
                                  CLUSTERING_INFO_MAPPING.MAPPING)

    chart_index = ClusterChartIndex(CLUSTERING_CHARTS_MAPPING.NAME,
                                    DOCUMENT_SETTING.SETTING,
                                    CLUSTERING_CHARTS_MAPPING.MAPPING)

    paragraphs_index = ClusterParagraphsIndex(CLUSTERING_PARAGRAPHS_MAPPING.NAME,
                                              DOCUMENT_SETTING.SETTING,
                                              CLUSTERING_PARAGRAPHS_MAPPING.MAPPING)

    if not ESIndex.CLIENT.indices.exists(index=CLUSTERING_INFO_MAPPING.NAME):
        info_index.create()
    else:
        info_index.delete_index()

    if not ESIndex.CLIENT.indices.exists(index=CLUSTERING_CHARTS_MAPPING.NAME):
        chart_index.create()
    else:
        info_index.delete_index()

    if not ESIndex.CLIENT.indices.exists(index=CLUSTERING_PARAGRAPHS_MAPPING.NAME):
        paragraphs_index.create()
    else:
        info_index.delete_index()

    info_index.bulk_insert_documents(clusters_info_list)
    chart_index.bulk_insert_documents(clusters_chart_list)
    paragraphs_index.bulk_insert_documents(clusters_paragraphs_list)


def apply():
    print("Start Clustering ...")

    FILTER_CONTENT_LENGTH = 50
    SUBJECT_FIELD_NAME = "keyword_main_subject"

    # -------------------------------- KMeans ----------------------------------

    # ----------------------------- Create Corpus-------------------------------

    print("create corpus ....")
    kmeans_main_corpus, kmeans_main_para_id_list, kmeans_main_paragraph_data_dict = create_corpus(SOURCE_ID,
                                                                                                  FILTER_CONTENT_LENGTH,
                                                                                                  SUBJECT_FIELD_NAME)

    # ----------------------------- Algorithm -------------------------------

    KMEANS_CONFIG = {
        "algorithm": "KMeans",
        "vector_type": "TFIDF",
        "feature_type": (1, 1),  # (min range, max range)
        "cluster_count_min": 5,
        "cluster_count_max": 50,
        "cluster_count_step": 5,
        "batch_size": 1024,
        "type_filter_query": "",
        "base_function": kmeans_clustering,
        "vector_extractor_function": tf_idf_embedding,
    }

    print("run clustering algorithm ....")
    kmeans_clusters_info_list, kmeans_clusters_chart_list, kmeans_clusters_paragraphs_list = KMEANS_CONFIG[
        "base_function"](kmeans_main_corpus,
                         kmeans_main_para_id_list,
                         kmeans_main_paragraph_data_dict,
                         KMEANS_CONFIG, SOURCE_ID)
    print("save to elastic ...")
    ingest_data_to_elastic(SOURCE_ID, kmeans_clusters_info_list, kmeans_clusters_chart_list,
                           kmeans_clusters_paragraphs_list)

    # ----------------------------------- LDA ----------------------------------

    # ----------------------------- Create Corpus-------------------------------

    print("create corpus ....")
    lda_main_corpus, lda_main_para_id_list, lda_main_paragraph_data_dict = create_corpus(SOURCE_ID,
                                                                                         FILTER_CONTENT_LENGTH,
                                                                                         SUBJECT_FIELD_NAME)

    LDA_CONFIG = {
        "algorithm": "LDA",
        "vector_type": "BOW",
        "feature_type": (1, 1),  # (min range, max range)
        "cluster_count_min": 5,
        "cluster_count_max": 50,
        "cluster_count_step": 5,
        "batch_size": 1024,
        "type_filter_query": "",
        "base_function": lda_clustering,
        "vector_extractor_function": tf_idf_embedding,
    }

    # ----------------------------- Algorithm -------------------------------

    print("run clustering algorithm ....")
    lda_clusters_info_list, lda_clusters_chart_list, lda_clusters_paragraphs_list = LDA_CONFIG["base_function"](
        lda_main_corpus,
        lda_main_para_id_list,
        lda_main_paragraph_data_dict,
        LDA_CONFIG, SOURCE_ID)
    print("save to elastic ...")
    ingest_data_to_elastic(SOURCE_ID, lda_clusters_info_list, lda_clusters_chart_list, lda_clusters_paragraphs_list)

    print("========================================\n\n", "Done\n\n\n========================================\n\n\n")
