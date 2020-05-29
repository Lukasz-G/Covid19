# -*- coding: utf-8 -*-

from PreProcessUtils import preprocess_metadata
from Pipeline_v19 import pipeline
    
def main():
    
    # Change this to whatever version of dataset we're on at this point
    version = "v19"
    
    # Enter the directory where the Kaggle dataset is saved
    directory = r"C:\Users\lga\Downloads\CORD-19-research-challenge"
    
    # Preprocess the metadata to get folder and subfolder structre and the names of files
    files, paths_to_files = preprocess_metadata(directory)
    
    #here it comes (or it should come...) the pathos parallel functions. 
    #'pipeline' function is supposed to be sent to each process and digest a sublist of
    
    pipeline(paths_to_files[:10],0)
    
           
if __name__ == "__main__":
    main()