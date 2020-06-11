# -*- coding: utf-8 -*-
from googletrans import Translator
import pandas as pd 
import os
import os.path
import numpy as np
import scispacy
import json
import spacy
from tqdm import tqdm
from scipy.spatial import distance
from scispacy.abbreviation import AbbreviationDetector
from spacy_langdetect import LanguageDetector
# UMLS linking will find concepts in the text, and link them to UMLS. 
from scispacy.umls_linking import UmlsEntityLinker
import time
from multiprocessing import Process, Queue, Manager
from multiprocessing.pool import Pool
from functools import partial
import re
import ast
import random
from langdetect import detect

from pandas.io.json import json_normalize

import uuid


def chunking(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def translate(text):
    translator=Translator()
    translation=translator.translate(str(text), dest='en').text
    return translation

# Returns a dictionary object that's easy to parse in pandas. For tables! :D
def extract_tables_from_json(js):
    json_list = []
    # Figures contain useful information. Since NLP doesn't handle images and tables,
    # we can leverage this text data in lieu of visual data.
    for figure in list(js["ref_entries"].keys()):
        json_dict = ["figref", figure, js["ref_entries"][figure]["text"]]
        json_dict.append(json_dict)
    return json_list

def init_filter_dict():
    inverse = dict() 
    d = {
        "discussion": ["conclusions","conclusion",'| discussion', "discussion",  'concluding remarks',
                       'discussion and conclusions','conclusion:', 'discussion and conclusion',
                       'conclusions:', 'outcomes', 'conclusions and perspectives', 
                       'conclusions and future perspectives', 'conclusions and future directions'],
        "results": ['executive summary', 'result', 'summary','results','results and discussion','results:',
                    'comment',"findings"],
        "introduction": ['introduction', 'background', 'i. introduction','supporting information','| introduction'],
        "methods": ['methods','method','statistical methods','materials','materials and methods', 'methods and materials',
                    'Methods', 'Materials and Methods',
                    'data collection','the study','study design','experimental design','objective',
                    'objectives','procedures','data collection and analysis', 'methodology',
                    'material and methods','the model','experimental procedures','main text',],
        "statistics": ['data analysis','statistical analysis', 'analysis','statistical analyses', 
                       'statistics','data','measures'],
        "clinical": ['clinical', 'diagnosis', 'diagnostic features', "differential diagnoses", 'classical signs','prognosis', 'clinical signs', 'pathogenesis',
                     'etiology','differential diagnosis','clinical features', 'case report', 'clinical findings',
                     'clinical presentation'],
        'treatment': ['treatment', 'interventions'],
        "prevention": ['epidemiology','risk factors'],
        "subjects": ['demographics','samples','subjects', 'study population','control','patients', 
                   'partisucipants','patient characteristics'],
        "animals": ['animals','animal models'],
        "abstract": ["abstract", 'a b s t r a c t','author summary'], 
        "review": ['review','literature review','keywords']}
    
    for key in d: 
        # Go through the list that is saved in the dict:
        for item in d[key]:
            # Check if in the inverted dict the key exists
            if item not in inverse: 
                # If not create a new list
                inverse[item] = [key] 
            else: 
                inverse[item].append(key) 
    return inverse

# Create instance of dictionary with alternative section names
inverted_dict = init_filter_dict()
    
# Get the section names using Brandon's original function
def get_section_name(text):
    text = text.lower()
    if len(text) == 0:
        return(text)
    if text in inverted_dict.keys():
        return(inverted_dict[text][0])
    else:
        if "case" in text or "study" in text: 
            return("methods")
        elif "clinic" in text:
            return("clinical")
        elif "stat" in text:
            return("statistics")
        elif "intro" in text or "backg" in text:
            return("introduction")
        elif "data" in text:
            return("statistics")
        elif "discuss" in text:
            return("discussion")
        elif "patient" in text:
            return("subjects")
        else: 
            return(text)

# Further clean up section names with an ad-hoc function
def further_clean_section(text):
    text = text.lower()
    if "methods" in text:
        text = "methods"
    elif "discussion" in text:
        text = "discussion"
    elif "introduction" in text:
        text = "introduction"
    elif "background" in text:
        text = "introduction"
    elif "conclusions" in text:
        text = "discussion"
    elif "results" in text:
        text = "results"
    elif "concluding remarks" in text:
        text = "discussion" 
    elif "conclusion" in text:
        text = "discussion"
    elif "a b s t r a c t" in text:
        text = "abstract"
    elif "diagnosis" in text:
        text = "clinical"
    elif "clinical signs" in text:
        text = "clinical"
    elif "statistical analysis" in text:
        text = "statistics" 
    return text

def init_nlp():
    spacy_nlp = spacy.load('en_core_sci_lg')
    new_vector = spacy_nlp(
                   """Positive-sense single‐stranded ribonucleic acid virus, subgenus 
                       sarbecovirus of the genus Betacoronavirus. 
                       Also known as severe acute respiratory syndrome coronavirus 2, 
                       also known by 2019 novel coronavirus. It is 
                       contagious in humans and is the cause of the ongoing pandemic of 
                       coronavirus disease. Coronavirus disease 2019 is a zoonotic infectious 
                       disease.""").vector    
    vector_data = {"COVID-19": new_vector,
                   "2019-nCoV": new_vector,
                   "SARS-CoV-2": new_vector}    
    for word, vector in vector_data.items():
            spacy_nlp.vocab.set_vector(word, vector)


    
    spacy_nlp.max_length=2000000

    # We also need to detect language, or else we'll be parsing non-english text 
    # as if it were English. 
    spacy_nlp.add_pipe(LanguageDetector(), name='language_detector', last=True)

    # Add the abbreviation pipe to the spacy pipeline. Only need to run this once.
    abbreviation_pipe = AbbreviationDetector(spacy_nlp)
    spacy_nlp.add_pipe(abbreviation_pipe)

    # Our linker will look up named entities/concepts in the UMLS graph and normalize
    # the data for us. 
    linker = UmlsEntityLinker(resolve_abbreviations=True)
    spacy_nlp.add_pipe(linker)
    
    
    return(spacy_nlp, linker)

def init_ner():
    models = ["en_ner_craft_md", "en_ner_jnlpba_md","en_ner_bc5cdr_md","en_ner_bionlp13cg_md"]
    nlps = [spacy.load(model) for model in models]
    return(nlps)

# Parse and process the metadata
def preprocess_metadata(directory):
    
    rows = []
    problem_rows = []

    

    df = pd.read_csv(os.path.join(directory,"metadata.csv"), low_memory=False) 
    df.reset_index(drop=True, inplace=True)
    sha_list = list(df.sha)
    folder_files = list(set(df.full_text_file.tolist()))
    #sanity check --> get rid of potential non-strings
    folder_files = [x for x in folder_files if type(x) == str]
    
    files_paths = []
    #single_folder = folder_files[0]
    for big_folder in folder_files:
        #folders are nested
        local_folder = os.path.join(directory, big_folder, big_folder)
        for f1 in os.listdir(local_folder):
            local_path = os.path.join(local_folder,f1)
            for f2 in os.listdir(local_path):
                files_paths.append((f2, os.path.join(local_path,f2)))
                #print(f2, local_path)
                #quit()
    
    sha_from_folders, paths_list = zip(*files_paths)
    
    #for x in sha_from_folders:
    #    if '.xml' in x:
    #        print(x)
    
    #split on '.' insteead os.path.splittext because of some "xml.json" formats
    sha_from_folders = [x.split('.')[0] for x in sha_from_folders]
    
    
    #sha_list, sha_from_folders = set(sha_list), set(sha_from_folders)
    #print(list(sha_list)[:10], list(sha_from_folders)[0])
    #print(sha_from_folders.difference(sha_list))
    #quit()        
        #print(f)
    #onlyfiles = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder,f))]
    
    #print(onlyfiles)
    #print(len(df))
    

    #print('Successfully processed {} rows'.format(len(output.index)))
    #print('{} rows could not be processed'.format(len(problem_rows)))
    #print('----')
    #print('{} unique articles in this dataset'.format(len(output.cord_uid.unique().tolist())))
    
    return sha_from_folders, paths_list

def parallelize_dataframe(df, func, n_cores=6, n_parts=400):
    df_split = np.array_split(df, n_parts)
    pool = Pool(n_cores)
    list(tqdm(pool.imap_unordered(func, df_split), total=len(df_split)))
    pool.close()
    pool.join()
                    
def init_list_cols():
    return ['GGP', 'SO', 'TAXON', 'CHEBI', 'GO', 'CL', 'DNA', 'CELL_TYPE', 'CELL_LINE', 'RNA', 'PROTEIN', 
                          'DISEASE', 'CHEMICAL', 'CANCER', 'ORGAN', 'TISSUE', 'ORGANISM', 'CELL', 'AMINO_ACID',
                          'GENE_OR_GENE_PRODUCT', 'SIMPLE_CHEMICAL', 'ANATOMICAL_SYSTEM', 'IMMATERIAL_ANATOMICAL_ENTITY',
                          'MULTI-TISSUE_STRUCTURE', 'DEVELOPING_ANATOMICAL_STRUCTURE', 'ORGANISM_SUBDIVISION',
                          'CELLULAR_COMPONENT', 'PATHOLOGICAL_FORMATION', "lemma", "UMLS","UMLS_ID"]


