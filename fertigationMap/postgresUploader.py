#!/usr/bin/python3

import pandas as pd
from sqlalchemy import create_engine, text

#run the first time only to create the table:

# Before running this script:
# --pandas version >1.4.0 needs to be installed

# Postgres:
postgreSQLTable = ['water_left_ru','water_left_bursa','water_left_ugent']
alchemyEngine   = create_engine('postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres', pool_recycle=3600);
file            = ['water_left_ru.txt','water_left_bursa.txt','water_left_ugent.txt']

for i in range(0,3):
    df = pd.read_csv(file[i], sep=",")
    #df = df.iloc[::6, :]   # select every nth row
    print(df)
    df.to_sql(postgreSQLTable[i], alchemyEngine, index=False, if_exists='replace');
