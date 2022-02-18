import requests
import pandas as pd
import sqlalchemy
import datetime
import json

def extract():
    API_key="reSaEDSoUNsqHdGzpIsp2dVGsZQCBOCC"
    response=requests.get(f"https://api.nytimes.com/svc/books/v3/lists/best-sellers/fiction.json?api-key={API_key}&offset=100")
    with open("rawdata.json", 'w') as f:
        json.dump(response.json(), f)

def transform():
    def extract_isbns(isbns_list):
        if not isbns_list:
            return None, None
        isbn10, isbn13=isbns_list[0].get('isbn10'), isbns_list[0].get('isbn13')
        return isbn10, isbn13

    with open("rawdata.json", 'r') as f:
        rawdata=json.load(f)

    best_sellers_df=pd.DataFrame(rawdata['results'])
    best_sellers_df=best_sellers_df[['title',	'description',	'contributor',	'publisher', 'isbns']]
    isbns=best_sellers_df['isbns'].apply(extract_isbns)
    best_sellers_df['isbn10']=list(map(lambda x:x[0], isbns))
    best_sellers_df['isbn13']=list(map(lambda x:x[1], isbns))
    best_sellers_df.drop(columns=['isbns'], inplace=True)
    best_sellers_df['date']=datetime.datetime.now().date()
    best_sellers_df.to_json("processed_data.json")

def load():
    best_sellers_df=pd.read_json("processed_data.json")
    engine=sqlalchemy.create_engine("sqlite:////home/mahmoud/nyt_books.db")
    best_sellers_df.to_sql("bestsellers", engine, index=False, if_exists='append')

def verify():
    engine=sqlalchemy.create_engine("sqlite:////home/mahmoud/nyt_books.db")
    loaded_data=pd.read_sql("select * from bestsellers;", con=engine)
    print("==> Loaded Data: ",loaded_data.shape, loaded_data.columns)