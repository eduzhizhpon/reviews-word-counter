import pandas as pd

df = pd.read_csv('amazon_review_full_csv/train.csv', header=None)
df = df.sample(frac=0.05)
df.to_csv('train-min.csv', index=False, header=False)