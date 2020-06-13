# -*- coding: utf-8 -*-
import random, argparse, os
from PreProcessUtils import preprocess_metadata, chunking
from Pipeline_v19 import pipeline
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count
from psutil import virtual_memory

def main():
    
    parser = argparse.ArgumentParser(description='Multi-CPU Preprocessing Pipeline for CORD19-v19.')
    parser.add_argument('--CORD19_path',
                    help='Path to CORD-19 path as provided on Kaggle platform.')
    parser.add_argument('--delta',
                    help='Delta json file with file names (CORD19 SHA) already preprocessed.')
    
    parser.add_argument('--max_n_files',
                    help='Optional maximal number of files you want to preprocess (for development purposes)')
    
    
    args = parser.parse_args()
    
    assert args.CORD19_path != None, "you should specify a path to CORD19 collection"
    
    if args.delta == None:
        print("No delta file specified. The pipeline will process the whole collection.")
    
    
    # Preprocess the metadata to get folder and subfolder structre and the names of files
    files, paths_to_files = preprocess_metadata(args.CORD19_path)
    
    #paths_to_files = paths_to_files[:100]
    #print(paths_to_files)
    file_size_list = [os.path.getsize(x) for x in paths_to_files]
    pathsAndFileSizes = list(zip(paths_to_files,file_size_list))
    paths_to_files_sorted, file_size_list_sorted = zip(*sorted(pathsAndFileSizes, key = lambda x: x[1]))
    paths_to_files_sorted = list(paths_to_files_sorted)
    random.shuffle(paths_to_files_sorted)
    
    if args.max_n_files != None:
        paths_to_files_sorted = paths_to_files_sorted[:args.max_n_files]
        
    #'pipeline' function is supposed to be sent to each process and digest a sublist of
    #paths_to_files = paths_to_files#[:1000]
    
    cpu_n = cpu_count()
    mem = virtual_memory()
    ram_size_gib= mem.free/1024**3 #or mem.total
    
    n_cpus_realistic = int(ram_size_gib/8)
    cpu_number = min(cpu_n,n_cpus_realistic)
    #print(cpu_number, "cpu_number")
    #quit()
    pool = ProcessPool(nodes=cpu_number)
    #paths_to_files = list(paths_to_files)
    #random.shuffle(paths_to_files)
    paths_chunks = chunking(paths_to_files_sorted,cpu_number)
    
    #pipeline(paths_to_files[:10],0)
    results = pool.map(pipeline, paths_chunks, list(range(cpu_number)))
    #pipeline(paths_to_files[:1000],0)
    
           
if __name__ == "__main__":
    main()