import pandas as pd


df = pd.ExcelFile('users.xls', engine='xlrd')
df = df.parse(sheet_name=0)
users_df = pd.DataFrame(df)

print(users_df)
