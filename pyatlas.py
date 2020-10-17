import pymongo
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class AtlasClient():
    #初期化
    def __init__(self, user_name, cluster_name, db_name, password):
        self.user_name = user_name
        self.db_name = db_name
        self.password = password
        self.client = pymongo.MongoClient(f"mongodb+srv://{user_name}:{password}@{cluster_name}.jipvx.mongodb.net/{db_name}?retryWrites=true&w=majority")

    #コレクション内容を取得してpd.DataFrameに格納
    #filter, projectionはこちら参照https://qiita.com/rsm223_rip/items/141eb146ad610215e5f7#%E6%A4%9C%E7%B4%A2%E6%96%B9%E6%B3%95
    def get_collection_to_df(self, collection_name, filter=None, projection=None):
        collection = self.client[self.db_name][collection_name]
        cursor = collection.find(filter=filter, projection=projection)
        df = pd.DataFrame(list(cursor))
        return df

    #前月データをCSV保存
    def backup_previous_month(self, collection_name, date_column, ref_time, output_dir):
        prev_month = ref_time - relativedelta(months=1)
        startdate = prev_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        enddate = ref_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        flt = {date_column:{"$gte":startdate, "$lt":enddate}}
        df = self.get_collection_to_df(collection_name, filter=flt)
        df.to_csv(f'{output_dir}/{startdate.strftime("%Y%m")}.csv', index=False)

    #コレクションを全て削除
    def drop_collection(self, collection_name):
        self.client[self.db_name][collection_name].remove()
    
    #コレクションからフィルタ条件で削除
    def delete_collection_data(self, collection_name, del_filter):
        collection = self.client[self.db_name][collection_name]
        collection.delete_many(del_filter)

    #コレクションから一定日以上前のデータを削除
    def delete_previous_data(self, collection_name, date_column, delete_days):
        del_end = datetime.now() - timedelta(days=delete_days)
        del_filter = {date_column:{"$lt":del_end}}
        self.delete_collection_data(collection_name, del_filter)