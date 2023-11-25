import pandas as pd
import os
import re
import psycopg2
from config import config

def process_raw(path):
    for file in os.listdir(path):
        df = pd.read_excel(path + file, header=2)
        df.columns = ['cic', 'industry', 'obs',
                    'employment', 'members', 'covered', 'percentmem', 'percentcov']
        df[['obs', 'employment', 'members', 'covered']] *= 1000
        df[['percentmem', 'percentcov']] *= 100
        df.drop(df.tail(3).index, axis=0, inplace=True)
        
        df[['employment', 'members', 'covered']] = df[['employment', 'members', 'covered']].astype(int)
        df[['percentmem', 'percentcov']] = df[['percentmem', 'percentcov']].astype(float).round(2)
        
        ptrn = re.compile(
            r'''(?P<year>[0-9]{4})'''
        )
        df['year'] = ptrn.search(file).group()
        for index in range(df['cic'].isna().sum()):
            df.fillna((index+1)*1000, limit=1, inplace=True)
        
        df['cic'] = df['cic'].astype(int)
        
        df.to_csv(f'./csv/{file[0:8]}.csv', index=False)

def load_sql(path):
    
    try:
        params = config()
    
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('TRUNCATE TABLE byIndustry;')
        
        for file in os.listdir(path):
            
            fullpath = os.path.abspath(path+file)
            sql = '''
                COPY byIndustry
                FROM STDIN
                DELIMITER ','
                CSV HEADER;
                '''
            cur.copy_expert(sql,
                            open(fullpath, 'r'))
        cur.execute(
            'SELECT * FROM byindustry ORDER BY percentmem DESC LIMIT 3;'
        )
        print(cur.fetchall())
        cur.close()
        conn.commit()
        conn.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


process_raw('./raw/')
load_sql('./csv/')