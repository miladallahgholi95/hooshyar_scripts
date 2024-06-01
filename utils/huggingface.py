from transformers import MT5ForConditionalGeneration, MT5Tokenizer, AutoTokenizer, AutoModelForTokenClassification, \
    pipeline, AutoModelForSequenceClassification
from sentence_transformers import models, SentenceTransformer
from input_configs import *

if RUN_SENTIMENT_MODULE:
    print("Load Sentiment Analysis Model")
    sentimentAnalyserTokenizer = MT5Tokenizer.from_pretrained("persiannlp/mt5-base-parsinlu-sentiment-analysis")
    sentimentAnalyserModel = MT5ForConditionalGeneration.from_pretrained("persiannlp/mt5-base-parsinlu-sentiment-analysis")
else:
    sentimentAnalyserTokenizer, sentimentAnalyserModel = None, None

if RUN_ENTITY_MODULE:
    print("Load Tagging Model")
    taggingSentenceTokenizer = AutoTokenizer.from_pretrained("HooshvareLab/bert-base-parsbert-ner-uncased")
    taggingSentenceModel = AutoModelForTokenClassification.from_pretrained("HooshvareLab/bert-base-parsbert-ner-uncased")
    taggingSentencePipeline = pipeline('ner', model=taggingSentenceModel, tokenizer=taggingSentenceTokenizer)
else:
    taggingSentenceTokenizer, taggingSentenceModel, taggingSentencePipeline = None, None, None

if RUN_SUBJECT_MODULE:
    print("Load Classification Model")
    classificationSentenceTokenizer = AutoTokenizer.from_pretrained("m3hrdadfi/albert-fa-base-v2-clf-persiannews")
    classificationSentenceModel = AutoModelForSequenceClassification.from_pretrained("m3hrdadfi/albert-fa-base-v2-clf-persiannews")
    classificationSentencePipeline = pipeline('text-classification', model=classificationSentenceModel,tokenizer=classificationSentenceTokenizer, top_k=None)
else:
    classificationSentenceTokenizer, classificationSentenceModel, classificationSentencePipeline = None, None, None

if RUN_VECTOR_MODULE:
    print("Load Vector Model")
    model_name_or_path = wikitriplet_model = 'm3hrdadfi/bert-fa-base-uncased-wikitriplet-mean-tokens'
    word_embedding_model = models.Transformer(model_name_or_path)
    pooling_model = models.Pooling(
        word_embedding_model.get_word_embedding_dimension(),
        pooling_mode_mean_tokens=True,
        pooling_mode_cls_token=False,
        pooling_mode_max_tokens=False)
    wikitriplet_model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
else:
    wikitriplet_model = None