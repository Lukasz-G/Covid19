# -*- coding: utf-8 -*-
import random, argparse, os, json
from PreProcessUtils import preprocess_metadata, chunking, str2bool
from Pipeline_v19 import pipeline
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
from psutil import virtual_memory



def main():
    
    parser = argparse.ArgumentParser(description='Multi-CPU Preprocessing Pipeline for CORD19-v19.')
    #path
    parser.add_argument('--CORD19_path', type=str,
                    help='Path to CORD-19 path as provided on Kaggle platform.')
    #optional delta json file
    parser.add_argument('--delta', type=str,
                    help='Delta json file with file names (CORD19 SHA) already preprocessed.')
    #max. number of files 
    parser.add_argument('--max_n_files', type=int, default=None, const=10, nargs='?',
                    help='Optional maximal number of files you want to preprocess (for development purposes)')
    #supposed/desired amount of free RAM per worker 
    parser.add_argument('--RAM_per_worker', type=int, default=12, const=12, nargs='?',
                    help='Amount of RAM in GiBs that each worker should obtain. It restricts the number of engaged workers. Default value 12 GiB')
    #en_core_sci_lg
    parser.add_argument('--en_core_sci_lg', type=str2bool, default=True, nargs='?', const=True,
                        help="A full spaCy pipeline for biomedical data with a larger vocabulary and 600k word vectors.")
    #en_ner_craft_md
    parser.add_argument('--en_ner_craft_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the CRAFT corpus.")
    #en_ner_jnlpba_md
    parser.add_argument('--en_ner_jnlpba_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the JNLPBA corpus.")
    #en_ner_bc5cdr_md
    parser.add_argument('--en_ner_bc5cdr_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the BC5CDR corpus.")
    #en_ner_bionlp13cg_md
    parser.add_argument('--en_ner_bionlp13cg_md', type=str2bool, default=True, nargs='?', const=True,
                        help="A spaCy NER model trained on the BIONLP13CG corpus.")
    
    
    args = parser.parse_args()
    
    assert args.CORD19_path != None, "you should specify a path to CORD19 collection"
    
    if args.delta == None:
        print("No delta file specified. The pipeline will process the whole collection.")
        delta_sha_list = False
    else:
        with open(args.delta) as f:
            delta_file = json.load(f)
            delta_sha_list = delta_file["delta list"]
            delta_sha_list = ''.join(delta_sha_list)
    
    # Preprocess the metadata to get folder and subfolder structre and the names of files
    files, paths_to_files = preprocess_metadata(args.CORD19_path)
    #files, paths_to_files = files[:100], paths_to_files[:100]
    if delta_sha_list:
        old_doc_nubmer = len(files)
        files_and_paths = [(file, file_path) for (file, file_path) in zip(files, paths_to_files) if file not in delta_sha_list]
        print(len(files_and_paths))
        files, paths_to_files = zip(*files_and_paths)
        new_doc_number = len(files)
        print("""
           A Delta file for CORD19 database has been applied. 
           Instead of {} files, only {} of them will be annotated. 
        """.format(old_doc_nubmer, new_doc_number))
        
    
    
    #quit()
    #paths_to_files = paths_to_files[:100]
    #print(paths_to_files)
    file_size_list = [os.path.getsize(x) for x in paths_to_files]
    pathsAndFileSizes = list(zip(paths_to_files,file_size_list))
    paths_to_files_sorted, file_size_list_sorted = zip(*sorted(pathsAndFileSizes, key = lambda x: x[1]))
    paths_to_files_sorted = list(paths_to_files_sorted)
    random.shuffle(paths_to_files_sorted)
    
    if args.max_n_files != None:
        paths_to_files_sorted = paths_to_files_sorted[:args.max_n_files]
    
    print("""
       We are going to process {} files from CORD19 database. 
    """.format(len(paths_to_files_sorted)))
       
    #'pipeline' function is supposed to be sent to each process and digest a sublist of
    cpu_n = cpu_count()
    mem = virtual_memory()
    ram_size_gib= mem.free/1024**3 #or mem.total
    
    n_cpus_realistic = int(ram_size_gib/args.RAM_per_worker)
    cpu_number = min(cpu_n,n_cpus_realistic)
    pool = ProcessPool(nodes=cpu_number)
    paths_chunks = chunking(paths_to_files_sorted,cpu_number)
    
    #gather SciSpacy model preferences and pack them into chunks for each process
    model_dict = {"description":"""
    A dictionary of SciSpacy model preferences specified by the user. 
    By default all models will be loaded"""}
    model_dict["en_core_sci_lg"] = args.en_core_sci_lg 
    model_dict["en_ner_craft_md"] = args.en_ner_craft_md 
    model_dict["en_ner_jnlpba_md"] = args.en_ner_jnlpba_md 
    model_dict["en_ner_bc5cdr_md"] = args.en_ner_bc5cdr_md
    model_dict["en_ner_bionlp13cg_md"] = args.en_ner_bionlp13cg_md
    
    models_selected = ["--"+k for k,v in model_dict.items() if type(v) == bool and v == True]
    
    print("""
       We have {} workers with at least {} GiB RAM free per worker.
       User-specified models for annotation:
       {}
       {}
       {}
       {}
       {}
    
    """.format(cpu_number, args.RAM_per_worker, *models_selected))
    
    #each process is supposed to receive one copy of the model_dict.
    model_preferences_list = [model_dict] * cpu_number
    #send file path chunks to a pool of workers
    results = pool.map(pipeline, paths_chunks, list(range(cpu_number)), model_preferences_list)
    
           
if __name__ == "__main__":
    main()