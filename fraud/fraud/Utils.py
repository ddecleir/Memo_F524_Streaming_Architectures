import numpy as np
import pandas as pd

def read_data(file):
    with open(file, 'r') as f:
        lines = f.readlines()
    # create an array that contains the data
    data = []
    for line in lines:
        temp = line[3:]
        line = temp[:-5]
        data.append(line)
    data = [x.strip().split(', ') for x in data]
    data = np.array(data)
    return pd.DataFrame(data)