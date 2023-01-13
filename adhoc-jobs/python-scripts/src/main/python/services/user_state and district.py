import pandas as pd

df_pandas = pd.read_csv("/home/raja/Downloads/wrong_mapped_device_id's - wrong_mapped_device_id's (1).csv")
L = df_pandas['device_id'].tolist()
print(type(L))
