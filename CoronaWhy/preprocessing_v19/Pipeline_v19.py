
def pipeline(paths_to_files, process_nb):
    
    #this function is supposed to run within multiprocessing
    #so the imports go this way...
    import os, json, uuid, re
    from tqdm import tqdm
    import numpy as np
    from langdetect import detect
    from PreProcessUtils import init_nlp, init_ner, translate, extract_tables_from_json, further_clean_section
    from collections import defaultdict
    
    folder_name = "preprocessed"#.format(process_nb)
    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
    
    
    nlp, linker = init_nlp()
    nlps = init_ner()
    
    
    
    
    
    
    languages = []
    start_chars = []
    end_chars = []
    entities = []
    texts = []
    lemmas = []
    vectors = []
    sections = []
    section_ids = []
    _ids = []
    columns = []
    
    translated = []
    umls_ids = []

    scispacy_ent_types = ['GGP', 'SO', 'TAXON', 'CHEBI', 'GO', 'CL', 'DNA', 'CELL_TYPE', 'CELL_LINE', 'RNA', 'PROTEIN', 
                          'DISEASE', 'CHEMICAL', 'CANCER', 'ORGAN', 'TISSUE', 'ORGANISM', 'CELL', 'AMINO_ACID',
                          'GENE_OR_GENE_PRODUCT', 'SIMPLE_CHEMICAL', 'ANATOMICAL_SYSTEM', 'IMMATERIAL_ANATOMICAL_ENTITY',
                          'MULTI-TISSUE_STRUCTURE', 'DEVELOPING_ANATOMICAL_STRUCTURE', 'ORGANISM_SUBDIVISION',
                          'CELLULAR_COMPONENT', 'PATHOLOGICAL_FORMATION']

    pbar = tqdm(total=len(paths_to_files))
    pbar.set_description("Preprocessing json files - Process no.{}: ".format(process_nb))
    
    
    #iterate over a list of file paths
    for path_to_file in paths_to_files:
        
        #create dict to store all annotations, lemmas, NERs etc...
        preprocessed_file = dict()
        #read in the json file
        with open(path_to_file, "rb") as f:
            json_file = json.load(f)
            f.close()
    
        #add paper id to file dict
        paper_id = json_file["paper_id"]
        preprocessed_file["paper_id"] = paper_id
        
        #try to get abstract if extisting
        try:
            paper_abstract = json_file["abstract"]#[abstract_paragraph for abstract_paragraph in json_file["abstract"]]
        except:
            paper_abstract = False
        preprocessed_file["abstract"] = []
        
        #get the body text being a list of sections
        body_text = json_file["body_text"]
        #we will gather section names and ids in a list
        preprocessed_file["text_body"] = []
        #add abstract paragraphs to text body and keep the track of them
        abstract_sections_nb = 0
        if paper_abstract:
            body_text = paper_abstract + body_text
            abstract_sections_nb = len(paper_abstract)
        
        #quick check if body text has at all some text
        all_text = ' '.join([x['text'] for x in body_text])
        if len(all_text) > 5:
            pass
        else:
            continue
            pass#add some actions if the file doesn't have any text
        
        
        
        
        #find out the language of the paper
        #what to do if language other than EN? Translate or omit...?
        try:
            found_lang = detect(all_text[:1000])
            preprocessed_file['language'] = found_lang
        except:
            print(all_text[:1000], 'some lang detect error')
            continue
        if found_lang != 'en':
            #if language other than EN then use Google translator to translate section by section
            original_text = '\n'.join(['\n'.join([section_body['section'], section_body['text']]) for section_body in body_text])
            preprocessed_file["original_text"] = original_text
            body_text = [{'text':translate(section_body['text']),'section':translate(section_body['section'])} for section_body in body_text]
        else:
            preprocessed_file["original_text"] = []
        
        
        #extract tables if applicable
        tables = extract_tables_from_json(json_file)
        preprocessed_file["tables"] = tables
        
        
        section_ids_list = []
        #iterate over a list of sections
        for section_nb, section_body in enumerate(body_text):
            #print("---new section---")
            #extract the text and the name of a section
            section_text = section_body['text']
            section_name = section_body['section']
            section_name = further_clean_section(section_name)
            section_id = str(uuid.uuid1())
            #section_dict["section_id"] = section_id
            
            section_ids_list.append(section_id)
            
            
            if section_nb < abstract_sections_nb:
                preprocessed_file["abstract"].append(section_id)
            else:
                preprocessed_file["text_body"].append({"section_id":section_id,"section_name":section_name})
                
            
            #let spacy digest the text content
            doc = nlp(section_text)
            
            #for sent in doc.sents:
            #    for ent in sent.ents:
            #        print(ent._.umls_ents)
            #continue
            
            #let unabbreviate the abbreviation
            if len(doc._.abbreviations) > 0:
                doc._.abbreviations.sort()
                join_list = []
                start = 0
                for abbrev in doc._.abbreviations:
                    join_list.append(str(doc.text[start:abbrev.start_char]))
                    if len(abbrev._.long_form) > 5: #Increase length so "a" and "an" don't get un-abbreviated
                        join_list.append(str(abbrev._.long_form))
                    else:
                        join_list.append(str(doc.text[abbrev.start_char:abbrev.end_char]))
                    start = abbrev.end_char
                # Reassign fixed body text to article in df.
                section_text = "".join(join_list)
                # We have new text. Re-nlp the doc for futher processing!
                doc = nlp(section_text)
            
            #uuid.uuid1
            #print(list(doc.ents))
            #
            #for ent in doc.ents:
            #    print(ent._.umls_ents)
                
            #for sent in doc.sents:
            #    for ent in sent.ents:
            #        print(ent._.umls_ents)
                #print(linker.umls.cui_to_entity[ent._.umls_ents[0][0]].canonical_name)
            #print(nlp.pipeline)
            #quit()
            #print(len(doc.sents))
            
            section_sent_ids_list = []
            for single_sentence in doc.sents:
                #print("---new sentence---")
                sentence_dict = {}
                sentence_id = str(uuid.uuid1())
                section_sent_ids_list.append(sentence_id)
                sentence_dict["sentence_id"] = sentence_id
                tokens = single_sentence.text
                sentence_dict["tokens"] = tokens
                #print(tokens)
                #quit()
                lemmas = [token.lemma_.lower() for token in single_sentence if not token.is_stop and re.search('[a-zA-Z]', str(token))]
                sentence_dict["lemmas"] = lemmas
                #print(tokens, lemmas)
                #quit()
                sent_ents = []
                for ent in single_sentence.ents: 
                    #print("normal ent: {}, umls: {}".format(ent,ent._.umls_ents))
                    if len(ent._.umls_ents) > 0:
                        #print(len(ent._.umls_ents))
                        poss = linker.umls.cui_to_entity[ent._.umls_ents[0][0]].canonical_name
                        #print(poss, '-----------------------')
                        #quit()
                        sent_ents.append(poss)
                    #sent_ents.append(ent.text)
                #quit()
                sentence_dict["umls"] = sent_ents
                
            
                umls_ids = [entity._.umls_ents[0][0] for entity in single_sentence.ents if len(entity._.umls_ents) > 0]
                sentence_dict["umls_ids"] = umls_ids
                
                sentence_vectors = [token.vector for token in single_sentence if not token.is_stop]
                if len(sentence_vectors)>0:
                    sentence_vector = np.stack(sentence_vectors, axis=0).sum(0).tolist()
                else:
                    sentence_vector = []
                sentence_dict["sent2vec"] = sentence_vector
                
                for special_nlp in nlps:
                    doc = special_nlp(section_text)
                    for single_sentence in doc.sents:
                        keys = list(set([ent.label_ for ent in single_sentence.ents]))
                        for key in keys:
                            if key not in scispacy_ent_types:
                                sentence_dict[key] = []
                            values = [ent.text for ent in single_sentence.ents if ent.label_ == key]
                            sentence_dict[key] = values
                            
                            
                #list_of_sent_dics.append(sentence_dict)
                
                preprocessed_file[sentence_id] = sentence_dict
            preprocessed_file[section_id] = section_sent_ids_list    
        
                                
            ######here I have stopped to adapt the code... last check point
            

        #pbar.update()
        with open(os.path.join(folder_name,paper_id+".json"), "w", encoding="utf-8") as f:
            json.dump(preprocessed_file, f, ensure_ascii=False)
            f.close()
        pbar.update()
        
    return True
               
