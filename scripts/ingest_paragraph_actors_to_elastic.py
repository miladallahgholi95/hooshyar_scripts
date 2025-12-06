from pathlib import Path

from elastic.connection import SEARCH_WINDOW_SIZE, ES_CLIENT, IndexObjectWithId
from elastic.SETTINGS import PARAGRAPH_SETTING
from elastic.MAPPINGS import PARAGRAPH_ACTOR_MAPPING, ACTORS_MAPPING, REGULATORS_MAPPING, PARAGRAPH_MAPPING
import enum

actor_duplicate = []

actor_duplicate_count = 0

class ConcatenateTypes(enum.Enum):
   FORM_KW = 'FORM_KW'
   KW_FORM = 'KW_FORM'
   
# main functions

def get_all_actors_list():

    actor_list = []

    res_query = {
        "match_all":{}
    }
    response = ES_CLIENT.search(
        index=ACTORS_MAPPING.NAME,
        query=res_query,
        size = 5000
    )

    actor_list = response['hits']['hits']
    return actor_list


def get_regulators_list():

    regulator_list = []

    res_query = {
        "exists":{
            "field":"related_actor"
        }
    }
    response = ES_CLIENT.search(
        index=REGULATORS_MAPPING.NAME,
        query=res_query,
        size = 1000
    )

    regulator_list = response['hits']['hits']
    return regulator_list


def get_role_keyword_list(file_name): 

    role_keywords_list = []

    role_keywords_file = str(Path("other_files/actor_patterns", file_name))

    with open(role_keywords_file, encoding="utf-8") as f:
        
        lines = f.readlines()

        for line in lines:
            line = line.strip()
            role_keywords_list.append(line)

    f.close()

    return role_keywords_list


def get_actor_forms(actor_form):
    forms =  [
        actor_form,
        actor_form + ' جمهوری اسلامی ایران',
        actor_form + ' ایران',
        actor_form + ' کشور',
        '(' + actor_form + ')',
        '(' + actor_form + 'جمهوری اسلامی ایران' + ')',
        '(' + actor_form + 'ایران' + ')',
        '(' + actor_form + 'کشور' + ')'
        '( ' + actor_form + ')',
        '( ' + actor_form + 'جمهوری اسلامی ایران' + ' )',
        '( ' + actor_form + 'ایران' + ' )',
        '( ' + actor_form + 'کشور' + ' )'
    ]
    
    return forms


def add_actors(paragraphs_dict, actor, actor_form, role_name, detection_type, response):
    global actor_duplicate_count
    global actor_duplicate
    
    if not response:
        return
    for para in response['hits']['hits']:
        paragraph_id = para['_id']
        if paragraph_id not in paragraphs_dict:
            paragraphs_dict[paragraph_id] = para
            
        if 'actors' not in paragraphs_dict[paragraph_id]['_source']:
            paragraphs_dict[paragraph_id]['_source']['actors'] = []

        detected_actor = {
            'id':actor['_id'],
            'name':actor['_source']['name'],
            'current_form':actor_form,
            'detection_type':detection_type,
            'role':role_name,
            'general_definition_keyword':None,
            'general_definition_text':None,
            'ref_paragraph_text':None,
        }
           
        # check if actor already exists in paragraph
        if detected_actor not in paragraphs_dict[paragraph_id]['_source']['actors']:
            # check if an actor's current_form is a substring of detected_actor's current_form
            for founded_actor in paragraphs_dict[paragraph_id]['_source']['actors']:
                if founded_actor['role'] == detected_actor['role'] and founded_actor['id'] == detected_actor['id']:
                    if founded_actor['current_form'] in detected_actor['current_form']:
                        # update actor's current_form
                        founded_actor['current_form'] = detected_actor['current_form']
                        break
                    elif detected_actor['current_form'] in founded_actor['current_form']:
                        break
            else:
                paragraphs_dict[paragraph_id]['_source']['actors'].append(detected_actor)
                
        # check if actor with same id and role already exists in paragraph more than once
        reapeat_count = 0
        for founded_actor in paragraphs_dict[paragraph_id]['_source']['actors']:
            if founded_actor['id'] == detected_actor['id'] and founded_actor['role'] == detected_actor['role']:
                reapeat_count += 1
        if reapeat_count > 1:
            paragraph_id = para['_id']
            actor_duplicate.append(paragraph_id)
            actor_duplicate_count += 1

       
def add_supervisor(paragraphs_dict, response):
    if not response:
        return
    
    for para_id, para in response.items():
        if paragraphs_dict[para_id]['_source'].get('supervisors') is None:
            paragraphs_dict[para_id]['_source']['supervisors'] = []
        for supervisor in para['supervisors']:
            # check if supervisor already exists in paragraph
            if supervisor not in paragraphs_dict[para_id]['_source']['supervisors']:
                paragraphs_dict[para_id]['_source']['supervisors'].append(supervisor)


def add_regulators(paragraphs_dict, regulator, tool_name, response):
    if not response:
        return
    for para in response['hits']['hits']:
        paragraph_id = para['_id']

        if paragraph_id not in paragraphs_dict:
            paragraphs_dict[paragraph_id] = para
            # paragraphs_dict[paragraph_id]['_source']['regulators'] = []

        if 'regulators' not in paragraphs_dict[paragraph_id]['_source']:
            paragraphs_dict[paragraph_id]['_source']['regulators'] = []

        detected_regulator = {
            'id':regulator['_id'],
            'name':regulator['_source']['name'],
            'tool_name':tool_name,
        }
        
        # check if actor already exists in paragraph
        if detected_regulator not in paragraphs_dict[paragraph_id]['_source']['regulators']:
            paragraphs_dict[paragraph_id]['_source']['regulators'].append(detected_regulator)       


                
def change_max_regex_length(length):
    ES_CLIENT.indices.put_settings(
        index=PARAGRAPH_MAPPING.NAME,
        body= {
            "index" : {
                "max_regex_length" : length
            }
        }
    )
   

def change_max_result_window(length,index_name):
    ES_CLIENT.indices.put_settings(
        index=index_name,
        body= {
            "index" : {
                "max_result_window" : length
            }
        }
    )
   
   
def add_field_not_exist(field,input_dict,default_value):
    if field not in input_dict:
        input_dict[field] = default_value
    return input_dict


def get_paras_with_search_after(index_name, last_doc_id="0"):
    res_query = {
        "match_all": {}
    }
    response = ES_CLIENT.search(
        index=index_name,
        query=res_query,
        size=5000,
        search_after=[last_doc_id] if last_doc_id != "0" else None,
        sort=[{"_id": {"order": "asc"}}]
    )
    
    last_doc_id = response['hits']['hits'][-1]['_id'] if len(response['hits']['hits']) > 0 else "0"

    return response, last_doc_id
    


# search functions


def simple_search(forms, role_keywords,
                    max_result_window = SEARCH_WINDOW_SIZE,
                    concatenate_type = ConcatenateTypes.FORM_KW
                  ):
    
    patterns = None

    if concatenate_type == ConcatenateTypes.FORM_KW:
        patterns = [(form + ' ' + role_keyword) for form in forms for role_keyword in role_keywords]
    elif concatenate_type == ConcatenateTypes.KW_FORM:
        patterns = [(role_keyword + ' ' + form) for form in forms for role_keyword in role_keywords]

    res_query = {
        "bool": {
            "should":[
                {
                    "match_phrase":{
                        "content":keyword
                    }
                } for keyword in patterns
            ],
            "minimum_should_match":1,
        }
    }
    
    highlight = {
        "fields": {
            "content": {
                "pre_tags": ["<span class='text-primary fw-bold'>"],
                "post_tags": ["</span>"],
                "number_of_fragments": 0  
            }
        }
    }
    
    
    response = ES_CLIENT.search(
        index=PARAGRAPH_MAPPING.NAME,
        query=res_query,
        size=max_result_window,
    )

    return response


def search_motevali(forms, role_keywords):
    plural_motevali_pattern_keywords = ['مکلفند','مکلف‌اند','موظفند','موظف‌اند']
    exeption_word = 'استثنای'
    patterns = [(form + ' ' + role_keyword) for form in forms for role_keyword in role_keywords]
    
    
    res_query = {
        "bool": {
            "must_not": [
                {
                    "match": {
                        "content": {
                        "query": exeption_word + " " + plural_keyword,
                        "operator": "and"
                        }
                    }
                } for plural_keyword in plural_motevali_pattern_keywords
            ],
            "should":[
                {
                    "match_phrase":{
                        "content":keyword
                    }
                } for keyword in patterns
            ],
            "minimum_should_match":1,
        }
    }
    response = ES_CLIENT.search(
        index=PARAGRAPH_MAPPING.NAME,
        query=res_query,
        size=SEARCH_WINDOW_SIZE,
    )
    
    return response


def search_eghdam_motevali(forms, role_keywords):
    eghdam_keywords = ['اقدام کند', 'اقدام‌کند', 'اقدام نماید', 'اقدام‌نماید']
    # remove duplicate eghdam_keywords
    eghdam_keywords = list(set([eghdam_keywords.replace('ئ','ی').replace('ـ','').replace('‌',' ') for eghdam_keywords in eghdam_keywords]))
    eghdam_patterns = [(form + ' ' + role_keyword) for form in forms for role_keyword in eghdam_keywords]
    salahiat_keywords = get_role_keyword_list('salahiatPatternKeywords.txt')
    # remove duplicate salahiat_keywords
    salahiat_keywords = list(set([salahiat_keywords.replace('ئ','ی').replace('ـ','').replace('‌',' ') for salahiat_keywords in salahiat_keywords]))
    non_eghdam_keywords = list(set(role_keywords) - set(eghdam_keywords)) + salahiat_keywords
    
    non_eghdam_patterns = [(form + ' ' + role_keyword) for form in forms for role_keyword in non_eghdam_keywords]

    # pattern = r'(' + '|'.join(forms) + r')\s+(\b(?!(?:\.|' + '|'.join(non_eghdam_keywords) + r')\b)\w+\b)\s+(' + '|'.join(eghdam_keywords) + r')\s+(\b(?!(?:\.|' + '|'.join(non_eghdam_keywords) + r')\b)\w+\b)\.'
        
    res_query = {
        "bool": {
            "should":[
                {
                    "match": {
                        "content": {
                        "query": eghdam_pattern,
                        "operator": "and"
                        }
                    } 
                } for eghdam_pattern in eghdam_patterns
            ],
            "must_not":[
                {
                    "match_phrase":{
                        "content":keyword
                    }
                } for keyword in non_eghdam_patterns
            ],
            "minimum_should_match":1,
        }
    }
    response = ES_CLIENT.search(
        index=PARAGRAPH_MAPPING.NAME,
        query=res_query,
        size=SEARCH_WINDOW_SIZE,
    )
    
    return response
    
    
def search_salahiat(forms, role_keywords):
    return simple_search(forms, role_keywords)


def search_ejaze_salahiat(forms, role_keywords):
    return simple_search(forms, role_keywords)
    
    
def search_hamkar(forms, role_keywords):
    patterns = [(role_keyword + ' ' + form) for form in forms for role_keyword in role_keywords]
    
    res_query = {
        "bool": {
            "should":[
                {
                    "match_phrase":{
                        "content":keyword
                    }
                } for keyword in patterns
            ],
            "minimum_should_match":1,
        }
    }
    response = ES_CLIENT.search(
        index=PARAGRAPH_MAPPING.NAME,
        query=res_query,
        size=SEARCH_WINDOW_SIZE,
    )
    
    return response


def search_supervisors(forms, role_keywords):
    supervisor_keywords = ['اعلام','ارائه','اقدام','ارسال', 'تقدیم']
    supervisor_verbs  = [
        'کند','کنند','نماید','نمایند','دهد','دهند'
        ,'شود','می شود','می‌شود','میشود'
    ]
    
    # remove duplicate supervisor_verbs
    supervisor_verbs = list(set([supervisor_verb.replace('ئ','ی').replace('ـ','').replace('‌',' ') for supervisor_verb in supervisor_verbs]))
    supervisor_patterns = [(supervisor_keyword + ' ' + supervisor_verb) for supervisor_keyword in supervisor_keywords for supervisor_verb in supervisor_verbs]
    
    # add second actor form
    patterns = ["به " + form for form in forms]
    
    res_query = {
        "bool": {
            "filter": [
                {
                    "nested": {
                        "path": "actors",
                        "query": {
                            "term": {
                                "actors.role": "متولی اجرا"
                            }
                        },
                        "inner_hits": {}
                    }
                }
            ],
            "must":[
                {
                    "match_phrase":{
                        "content":"گزارش"
                    }
                }
            ],
            "should":[
                {
                    "match_phrase":{
                        "content":keyword
                    }
                } for keyword in patterns
            ],
            "minimum_should_match":1,
        }
    }
    
    response = ES_CLIENT.search(
        index=PARAGRAPH_ACTOR_MAPPING.NAME,
        query=res_query,
        size=5000
    )
    
    return response
    
    # search for supervisor_patterns in responsed paragraphs with its _id
    # para_ids = [para['_id'] for para in response['hits']['hits']]

    # if not para_ids:
    #     return response
    
    # res_query = {
    #     "bool": {
    #         "filter": [{
    #             "terms": {
    #                 "_id": para_ids
    #             }
    #         }],
    #         "should":[
    #             {
    #                 "match_phrase":{
    #                     "content":keyword
    #                 }
    #             } for keyword in supervisor_patterns
    #         ],
    #         "minimum_should_match":1,
    #     }
    # }
    # response = ES_CLIENT.search(
    #     index=PARAGRAPH_ACTOR_MAPPING.NAME,
    #     query=res_query,
    #     size=5000,
    # )
    
    # return response


def search_regulators(forms, role_keywords):
    return simple_search(forms, role_keywords,concatenate_type=ConcatenateTypes.KW_FORM)


# analyzers 

def analyze_supervisors(response, supervisor, supervisor_form):
    if not response:
        return response
    
    supervisor_keywords = ['اعلام','ارائه','اقدام','ارسال', 'تقدیم']
    supervisor_verbs  = [
        'کند','کنند','نماید','نمایند','دهد','دهند'
        ,'شود','می شود','می‌شود','میشود'
    ]
    
    # remove duplicate supervisor_verbs
    supervisor_keywords = list(set([supervisor_keyword.replace('ئ','ی').replace('ـ','').replace('‌',' ') for supervisor_keyword in supervisor_keywords]))
    supervisor_verbs = list(set([supervisor_verb.replace('ئ','ی').replace('ـ','').replace('‌',' ') for supervisor_verb in supervisor_verbs]))
    supervisor_patterns = [(supervisor_keyword + ' ' + supervisor_verb) for supervisor_keyword in supervisor_keywords for supervisor_verb in supervisor_verbs]
    
    res = {}
    paragraphs = response['hits']['hits']
    
    for para in paragraphs:
        if para.get('inner_hits') is None:
            continue
        actors = para['inner_hits']['actors']['hits']['hits']
        content = para['_source']['content']
        sentences = content.split('.')
        # strip sentences
        for sentence in sentences:
            sentence = sentence.strip()
            sentence = sentence.replace('ئ','ی').replace('ـ','').replace('‌',' ').replace('ي','ی')
            # check whather sentence contains supervisor
            if 'گزارش' in sentence:
                for actor in actors:
                    # remove duplicate supervisor_form
                    supervisor_form = supervisor_form.replace('ئ','ی').replace('ـ','').replace('‌',' ').replace('ي','ی')
                    supervisor_name = supervisor['_source']['name']
                    supervisor_id = supervisor['_id']
                    # find supervisor_form in sentence
                    supervisor_form_index = sentence.find("به " + supervisor_form)
                    # find actor_form in sentence
                    actor_form_index = sentence.find(actor['_source']['current_form'])
                    # find supervisor_pattern in sentence
                    supervisor_pattern_indexes = [sentence.find(supervisor_pattern) for supervisor_pattern in supervisor_patterns]
                    supervisor_pattern_indexes = [index for index in supervisor_pattern_indexes if index != -1]
                    if len (supervisor_pattern_indexes) == 0:
                        continue
                    # check if supervisor_form is after actor_form and pattern is after both of them
                    if supervisor_form_index > actor_form_index and \
                        supervisor_form_index != -1 and actor_form_index != -1 and \
                        any(supervisor_pattern_index > supervisor_form_index for supervisor_pattern_index in supervisor_pattern_indexes):
                        # add para to res
                        if para['_id'] not in res:
                            res[para['_id']] = para
                            res[para['_id']]['supervisors'] = []
                        # add supervisor to para
                        supervisor_obj = {
                            "source_actor_id":actor['_source']['id'],
                            "source_actor_name":actor['_source']['name'],
                            "source_actor_form":actor['_source']['current_form'],
                            "supervisor_actor_id":supervisor_id,
                            "supervisor_actor_name": supervisor_name,
                            "supervisor_actor_form": supervisor_form,
                        }
                        # check if supervisor already exists in paragraph and source_actor_name is not supervisor_actor_name
                        if supervisor_obj not in res[para['_id']]['supervisors'] and \
                            supervisor_obj['source_actor_name'] != supervisor_obj['supervisor_actor_name']:
                            res[para['_id']]['supervisors'].append(supervisor_obj)
                        
    return res
                
                
def analyze_eghdam_motevali(response, actor_form, role_keywords):
    if not response:
        return response
    
    res = []
    paragraphs = response['hits']['hits']
    
    for para in paragraphs:
        content = para['_source']['content']
        sentences = content.split('.')
        # split sentences with :
        colon_split_sentences = []
        for sentence in sentences:
            colon_split_sentences += sentence.split(':')
        # split sentences with ،
        comma_split_sentences = []
        for sentence in colon_split_sentences:
            comma_split_sentences += sentence.split('،')
            
        # strip sentences
        comma_split_sentences = [sentence.strip() for sentence in comma_split_sentences]
        
        for sentence in comma_split_sentences:
            # check if colon_sentence contains role_keywords and actor_form in the first 40 characters
            if any(role_keyword in sentence for role_keyword in role_keywords) and any(actor_form in sentence[0:40] for actor_form in actor_form):
                res.append(para)
                print("-------------------------------------")
                break
            
    response['hits']['hits'] = res
                
    return response
            


    
EJAZE_SALAHIA = [
    'اجازه داده می‌شود',
    'اجازه داده می شود',
    'اجازه داده  میشود'
]

ALL_PATTERNS = {
    'base_motevali':{
        'role_name':'متولی اجرا',
        'detection_type':'صریح',
        'role_keywords':get_role_keyword_list('motevalianPatternKeywords.txt'),
        'search_function':search_motevali,
        'add_function':add_actors
    },
    'eghdam_motevali':{
        'role_name':'متولی اجرا',
        'detection_type':'صریح',
        'role_keywords':get_role_keyword_list('motevalianPatternKeywords.txt'),
        'search_function':search_eghdam_motevali,
        'add_function':add_actors
    },
    'salahiat':{
        'role_name':'دارای صلاحیت اختیاری',
        'detection_type':'صریح',
        'role_keywords':get_role_keyword_list('salahiatPatternKeywords.txt'),
        'search_function':search_salahiat,
        'add_function':add_actors
    },
    'ejaze_salahiat':{
        'role_name':'دارای صلاحیت اختیاری',
        'detection_type':'صریح',
        'role_keywords':EJAZE_SALAHIA,
        'search_function':search_ejaze_salahiat,
        'add_function':add_actors
    },
    'hamkar':{
        'role_name':'همکار',
        'detection_type':'صریح',
        'role_keywords':get_role_keyword_list('hamkaranPatternKeywords.txt'),
        'search_function':search_hamkar,
        'add_function':add_actors
    },
    # 'supervisors':{
    #     'role_name':'متولی اجرا',
    #     'detection_type':'صریح',
    #     'role_keywords':get_role_keyword_list('motevalianPatternKeywords.txt'),
    #     'search_function':search_supervisors,
    #     'add_function':add_supervisor
    # },
    'regulator_mojavez':{
        'tool_name':'مجوز',
        'role_keywords':get_role_keyword_list('regulator_mojavez_keywords.txt'),
        'search_function':search_regulators,
        'add_function':add_regulators
    } 
}


def apply():
    global actor_duplicate_count
    global actor_duplicate

    # change max_regex_length to 10000
    change_max_regex_length(10000)
    
    # get all actors
    actors_list = get_all_actors_list()
    regulator_name_list = [regulator['_source']['related_actor'] for regulator in get_regulators_list()]

    result_ingest_list = []
    paragraphs_dict = {}
    
    iteration_cnt = 0
    for actor in actors_list:
        print("1", iteration_cnt/len(actors_list))
        iteration_cnt += 1

        # get actor forms and remove duplicate forms
        actor_forms_list = actor['_source']['forms']
        actor_forms_list = list(set([actor_form.replace('ئ','ی').replace('ـ','').replace('‌',' ').replace("  ", " ") for actor_form in actor_forms_list ]))
        
        # loop over actor forms
        for actor_form in actor_forms_list:
            # get forms
            forms = get_actor_forms(actor_form)
            
            for pattern_name, pattern in ALL_PATTERNS.items():
                # remove duplicate role_keywords
                role_keywords = list(set(pattern.replace('ئ','ی').replace('ـ','').replace('‌',' ') for pattern in pattern['role_keywords']))
                
                # search pattern in paragraphs
                if pattern_name == 'regulator_mojavez':
                    if actor['_source']['name'] in regulator_name_list:   
                        response = pattern['search_function'](forms, role_keywords)
                    else:
                        continue
                else:
                    response = pattern['search_function'](forms, role_keywords)
                
                # analyze response (for eghdam_motevali)
                if pattern_name == 'eghdam_motevali':
                    old_role_keywords = pattern['role_keywords']
                    response = analyze_eghdam_motevali(response, forms, old_role_keywords)

                # add actor if pattern found in paragraphs
                if pattern_name == 'regulator_mojavez':
                    pattern['add_function'](paragraphs_dict, actor, 'مجوز', response)
                else:
                    pattern['add_function'](paragraphs_dict, actor, actor_form, pattern['role_name'], pattern['detection_type'], response)


    # convert paragraphs_dict to list
    for paragraph_id, paragraph_obj in paragraphs_dict.items():
        paragraph_obj['_source'] = add_field_not_exist('actors',paragraph_obj['_source'],[])
        paragraph_obj['_source'] = add_field_not_exist('regulators',paragraph_obj['_source'],[])
        paragraph_obj['_source'] = add_field_not_exist('supervisors',paragraph_obj['_source'],[])
        paragraph_obj['_source']["_id"] = paragraph_obj["_id"]
        result_ingest_list.append(paragraph_obj["_source"])
        
    
    # ingest result_ingest_list to paragraph_actor_index
    new_index = IndexObjectWithId(PARAGRAPH_ACTOR_MAPPING.NAME,
                                    PARAGRAPH_SETTING.SETTING,
                                    PARAGRAPH_ACTOR_MAPPING.MAPPING)
    new_index.create()
    new_index.bulk_insert_documents(result_ingest_list)
    
    # print duplicate info
    print(f"actor_duplicate = {actor_duplicate}")
    print(f"actor_duplicate_count = {actor_duplicate_count}")
    
    # change max_regex_length to default value
    change_max_regex_length(1000)
    
    apply_supervisors()


def apply_supervisors():
    global DOTIC_SOURCE_ID 
    DOTIC_SOURCE_ID = "1"
    
    # get all actors
    actors_list = get_all_actors_list()
    
    change_max_result_window(100000, PARAGRAPH_ACTOR_MAPPING.NAME)
    
    all_paragraphs = []
    paragraphs_dict = {}
    
    last_doc_id = "0"
    while True:
        response, last_doc_id = get_paras_with_search_after(PARAGRAPH_ACTOR_MAPPING.NAME, last_doc_id)
        all_paragraphs += response['hits']['hits']
        if last_doc_id == "0":
            break
        
    # fill paragraphs_dict
    for para in all_paragraphs:
        paragraphs_dict[para['_id']] = para
        
    print(len(paragraphs_dict))
    
    # find supervisors in paragraphs
    iteration_cnt = 0
    for actor in actors_list:
        print("2", iteration_cnt/len(actors_list))
        iteration_cnt += 1

        # get actor forms and remove duplicate forms
        actor_forms_list = actor['_source']['forms']
        actor_forms_list = list(set([actor_form.replace('ئ','ی').replace('ـ','').replace('‌',' ').replace("  ", " ") for actor_form in actor_forms_list ]))
        
        # loop over actor forms
        for actor_form in actor_forms_list:
            # get forms
            forms = get_actor_forms(actor_form)
            
            # search supervisors in paragraphs
            response = search_supervisors(forms, [])
            
            # analyze response
            response = analyze_supervisors(response, actor, actor_form)
            
            # add supervisors
            add_supervisor(paragraphs_dict, response)
            
    # convert paragraphs_dict to list
    result_ingest_list = []
    for paragraph_id, paragraph_obj in paragraphs_dict.items():
        paragraph_obj['_source'] = add_field_not_exist('supervisors',paragraph_obj['_source'],[])
        paragraph_obj['_source']["_id"] = paragraph_obj["_id"]
        result_ingest_list.append(paragraph_obj["_source"])

    # ingest result_ingest_list to paragraph_actor_index
    new_index = IndexObjectWithId(PARAGRAPH_ACTOR_MAPPING.NAME,
                                    PARAGRAPH_SETTING.SETTING,
                                    PARAGRAPH_ACTOR_MAPPING.MAPPING)
    new_index.create()
    new_index.bulk_insert_documents(result_ingest_list)
        
    # change max_result_window to default value
    change_max_result_window(SEARCH_WINDOW_SIZE, PARAGRAPH_ACTOR_MAPPING.NAME)