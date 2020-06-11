# -*- coding: utf-8 -*-
import random
from PreProcessUtils import preprocess_metadata, chunking
from Pipeline_v19 import pipeline
from pathos.pools import ProcessPool
from pathos.helpers import cpu_count

def main():
    
    # Change this to whatever version of dataset we're on at this point
    version = "v19"
    
    # Enter the directory where the Kaggle dataset is saved
    directory = r"C:\Users\lga\Downloads\CORD-19-research-challenge"#C:\Users\lga\Downloads\CORD-19-research-challenge
    
    # Preprocess the metadata to get folder and subfolder structre and the names of files
    files, paths_to_files = preprocess_metadata(directory)
    
    #here it comes (or it should come...) the pathos parallel functions. 
    #'pipeline' function is supposed to be sent to each process and digest a sublist of
    paths_to_files = paths_to_files[:1000]
    
    cpu_n = cpu_count()
    pool = ProcessPool(nodes=2)
    paths_to_files = list(paths_to_files)
    #random.shuffle(paths_to_files)
    paths_chunks = chunking(paths_to_files,cpu_n)
    
    #pipeline(paths_to_files[:10],0)
    results = pool.map(pipeline, paths_chunks, list(range(cpu_n)))
    #pipeline(paths_to_files[:1000],0)
    
           
if __name__ == "__main__":
    main()