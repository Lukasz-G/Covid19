
def pipeline(paths_to_files, process_nb):
    
    #this function is supposed to run within multiprocessing
    #so the imports go this way...
    import os, json
    from tqdm import tqdm
    from langdetect import detect
    from PreProcessUtils import init_nlp, init_ner, translate
    
    if not os.path.exists("preprocessed"):
        os.mkdir("preprocessed")
    
    
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
            paper_abstract = json_file["abstract"][0]['text']
        except:
            paper_abstract = False
        #get the body text being a list of sections
        body_text = json_file["body_text"]
        
        #quick check if body text has at all some text
        all_text = ' '.join([x['text'] for x in body_text])
        if len(all_text) > 5:
            pass
        else:
            pass#add some actions if the file doesn't have any text
        
        #find out the language of the paper
        #what to do if language other than EN? Translate or omit...?
        found_lang = detect(all_text[:100])
        preprocessed_file['language'] = found_lang
        if found_lang != 'en':
            #if language other than EN then use Google translator to translate section by section
            body_text = [{'text':translate(section_body['text']),'section':translate(section_body['section'])} for section_body in body_text]
        
        
        #iterate over a list of sections
        for section_body in body_text:
            #extract the text and the name of a section
            section_text = section_body['text']
            section_name = section_body['section']
            
            #let spacy digest the text content
            doc = nlp(section_text)
            
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
                new_text = "".join(join_list)
                # We have new text. Re-nlp the doc for futher processing!
                doc = nlp(new_text)
    
            
            
            ######here I have stopped to adapt the code... last check point

            lemmas.append([token.lemma_.lower() for token in doc if not token.is_stop and re.search('[a-zA-Z]', str(token))])
                doc_ents = []
                for ent in doc.ents: 
                    if len(ent._.umls_ents) > 0:
                        poss = linker.umls.cui_to_entity[ent._.umls_ents[0][0]].canonical_name
                        doc_ents.append(poss)
                entities.append(doc_ents)
                umls_ids.append([entity._.umls_ents[0][0] for entity in doc.ents if len(entity._.umls_ents) > 0])
                _ids.append(df.iloc[i]["cord_uid"])
                sections.append(df.iloc[i]["section"])
                section_ids.append(df.iloc[i]["section_uid"])
    
            else:   
                try: 
                    text = translate(df.iloc[i]["text"])
                    doc = nlp(str(df.iloc[i]["text"]))
    
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
                        new_text = "".join(join_list)
                        # We have new text. Re-nlp the doc for futher processing!
                        doc = nlp(new_text)
    
                    if len(doc.text) > 5:
                        languages.append(doc._.language["language"])
                        vectors.append(doc.vector)
                        translated.append(True)
                        sections.append(df.iloc[i]["section"])
                        section_ids.append(df.iloc[i]["section_uid"])
    
                        lemmas.append([token.lemma_ for token in doc if not token.is_stop and re.search('[a-zA-Z]', str(token))])
                        for ent in doc.ents: 
                            if len(ent._.umls_ents) > 0:
                                poss = linker.umls.cui_to_entity[ent._.umls_ents[0][0]].canonical_name
                                entities.append(poss)
                        umls_ids.append([entity._.umls_ents[0][0] for entity in doc.ents if len(entity._.umls_ents) > 0])
                        entities.append(doc_ents)
                        _ids.append(df.iloc[i]["cord_uid"])
                        sections.append(df.iloc[i]["section"]) ######
                        section_ids.append(df.iloc[i]["section_uid"])
    
                except:
                    entities.append("[]")
                    translated.append(False)
                    vectors.append(np.zeros(200))
                    lemmas.append("[]")
                    _ids.append(df.iloc[i,0])
                    umls_ids.append("[]")
                    languages.append(doc._.language["language"])
                    section_ids.append(df.iloc[i]["section_uid"])
                    sections.append(df.iloc[i]["section"])

        pbar.update()
    
    li1 = _ids
    li2 = sections
    li3 = [i for i in range(len(entities))]

    #     sentence_id = [str(x) + str(y) + str(z)  for x,y,z in zip(li1,li2,li3)]

    new_df = pd.DataFrame(data={"cord_uid": _ids,   
                                "section_uid": section_ids, 
                                "section": sections, 
                                "lemma": lemmas, 
                                "UMLS": entities, 
                                "UMLS_IDS": umls_ids, 
                                "w2vVector": vectors, 
                                "translated":translated})

    for col in scispacy_ent_types:
        new_df[col] = "[]"
    for j in tqdm(new_df.index):
        for nlp in nlps:
            doc = nlp(str(new_df.iloc[j]["section"]))
            keys = list(set([ent.label_ for ent in doc.ents]))
            for key in keys:

                # Some entity types are present in the model, but not in the documentation! 
                # In that case, we'll just automatically add it to the df. 
                if key not in scispacy_ent_types:
                    new_df = pd.concat([new_df,pd.DataFrame(columns=[key])])
                    new_df[key] = "[]"

                values = [ent.text for ent in doc.ents if ent.label_ == key]
                new_df.at[j,key] = values


    new_df["w2vVector"] = [np.asarray(a=i, dtype="float64") for i in new_df["w2vVector"].to_list()]


    new_df.to_pickle("df_parts/" + new_df.iloc[0]["section_uid"] + ".pickle", compression="gzip")            
