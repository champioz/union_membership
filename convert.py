import pandas as pd
import numpy as np
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
        df['category'] = np.nan
        
        # Add categories, and then remove 
        # categories which only serve as
        # aggregates - i.e., keep a category
        # only if it has no subcategories
        
        # Record categories with no subcategories
        cicna = np.where(df['cic'].isna())[0]
        
        # Record full list of categories, including
        # those that will be kept
        indcaps = df[df.industry.str.contains(
            r'''^[A-Z\s,]+$'''
        )].index.tolist()
        
        # Add appropriate category to each row
        for ind, na_index in enumerate(indcaps):
            
            category = df['industry'][na_index]
            
            if (ind + 1) == len(indcaps):
                df.loc[na_index:, 'category'] = category
                break
            
            next_index = indcaps[ind+1]
            df.loc[na_index:next_index-1, 'category'] = category
            
        # Drop appropriate categories
        df.drop(cicna, axis=0, inplace=True)
        
        df.to_csv(f'./csv/{file[0:8]}.csv', index=False)


def load_sql(path):
    
    try:
        params = config()
    
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute('TRUNCATE TABLE byIndustry;')
        
        for file in os.listdir(path):
            
            sql = '''
                COPY byIndustry
                FROM STDIN
                DELIMITER ','
                CSV HEADER;
                '''
            cur.copy_expert(sql,
                            open(path + file, 'r'))
        
        cur.close()
        conn.commit()
        conn.close()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


process_raw('./raw/')
load_sql('./csv/')