from pathlib import Path
import numpy as np

def load_file(n, path_file, file_format, press='*.'):
    """
    **** We can do data-processing tasks with n batches of csv files at the same time in Ariflow ****
    Function split list of csv files into list of batches, each batch contain list of csv files
    
    """
    n = int(n)
    ls_1 = list(Path(path_file).glob(press+file_format)) 
    return list(map(list,np.array_split(ls_1, n)))