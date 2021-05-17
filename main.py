import count_word as cw
import pandas as pd

def main():
    dataframe = cw.read_csv('train-min.csv')
    cw.split_review(dataframe)
    file_path = cw.read_files_path('./split_csv')

    # cw.n_core = 5
    cw.load_review_data(file_path)

    cw.start_serial()
    cw.start_process()

def get_sample():
    dataframe = pd.read_csv('amazon_review_full_csv/train.csv', header=None)
    dataframe = dataframe[:int(dataframe.shape[0]/100)]
    dataframe.to_csv('train-min.csv', index=False, header=False)
    print(f'Sample [{dataframe.shape[0]}] complete!!')

if __name__ == '__main__':
    # get_sample()
    main()