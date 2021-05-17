import pandas as pd
import numpy as np
import os
import time

import multiprocessing
from collections import Counter

# Cantidad de Cores a ejecutar
n_core = 2

# Lista de Procesos con cada tipo de review
review_process = list()

# Lista con caracteres especiales
_Review__characters = [',', '.', '(', ')', '?', '!', '\"', '-']

# Lista con las stopword
# stopwords_path = 'stopwords/stop_words_en_nltk.txt'
stopwords_path = 'stopwords/stop_words_en_1298.txt'
# stopwords_path = 'stopwords/stop_words_en_nltk.txt'

_Review__stopwords = list()
with open(stopwords_path) as f:
    lines = f.readlines()
    for line in lines:
        _Review__stopwords.extend([line.replace("\n", "")])

class Review():

    most_common_word = None

    def __init__(self, dataset):
        self.dataset = dataset
    
    def count_words(self):
        self.most_common_word = list()
        for index, row in self.dataset.iterrows():
            word_string = str(row.iloc[2]).lower()
            for character in __characters:
                word_string = word_string.replace(character, "")

            word_string = word_string.split()
            for w in word_string:
                if w not in __stopwords:
                    self.most_common_word.append(w)
        counter = Counter(self.most_common_word)
        self.most_common_word = counter.most_common(10)
    
    def get_characters(self):
        return __characters

class ReviewProcess(multiprocessing.Process):

    def __init__(self, review_list):
        multiprocessing.Process.__init__(self)
        self.review_list = review_list
    
    def run(self):
        for r in self.review_list:
            r.count_words()
        return


def load_review_data(file_path):
    global review_process

    review_process = list()
    for f in file_path:
        df = read_csv(f)
        review_process.append(Review(df))
    
def start_serial():
    start_time = time.time()
    for r in review_process:
        r.count_words()
    print(f'Tiempo Serial: {time.time() - start_time}')
    for r in review_process:
        print(r.most_common_word)

def start_process():
    review_len = len(review_process)
    start_time = time.time()
    n = int(review_len/n_core)
    start_index = 0
    end_index = n
    review_process_list = list()
    for i in range(n_core):
        if i == n_core - 1:
            end_index = review_len
        r_p = review_process[start_index:end_index]
        review_process_item = ReviewProcess(r_p)
        review_process_item.start()
        review_process_list.append(review_process_item)
        start_index = end_index
        end_index += n

    for r_p in review_process_list:
        r_p.join()

    print(f'Tiempo por Procesos: {time.time() - start_time}')
    for r in review_process:
        print(r.most_common_word)
    

def split_review(df):
    review_id = df.iloc[:, 0].values
    review_id = np.unique(review_id)

    if not os.path.exists('split_csv'):
        os.makedirs('split_csv')

    for r_id in review_id:
        data_by_id = df[df.iloc[:, 0] == r_id]
        path = 'split_csv/train-' + str(r_id) + '.csv'
        data_by_id.to_csv(path, index=False, header=False)

def read_files_path(root_path, absolute=True):
    file_path = list()
    for f in os.listdir(root_path):
        if absolute:
            path = os.path.abspath(root_path + "/" + f)
        else:
            path = root_path + "/" + f
        file_path.append(path)
    return file_path

def read_csv(path):
    return pd.read_csv(path, header=None)