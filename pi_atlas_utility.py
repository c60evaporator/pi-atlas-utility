from pyatlas import AtlasClient
from datetime import datetime
import configparser
import logging
from pit import Pit
import ast
import os

#処理名一覧
PROCESS_NAME_LIST = ['Backup','Delete']

class PiAtlasUtility():
    #初期化
    def __init__(self, masterdate = datetime.today()):
        self.masterdate = masterdate
        self.backup_dir = None
        self.process_name = None
        self.delete_day = None

    #処理本体を実行
    def _run_process(self, user_name, cluster_name, db_name, collection_name, retry):
        pa = Pit.get('atlas')[':pa']
        num = Pit.get('atlas')[':num']
        pad = ''.join([chr(ord(a) + num + 5) for a in pa])
        for i in range(retry):
            try:
                atlasclient = AtlasClient(user_name=user_name, cluster_name=cluster_name, db_name=db_name, password=pad)
                #バックアップ処理
                if self.process_name == 'Backup':
                    atlasclient.backup_previous_month(collection_name, "Date_Master", datetime.now(), self.backup_dir)
                #一定日以上前のデータを削除
                elif self.process_name == 'Delete':
                    atlasclient.delete_previous_data(collection_name, "Date_Master", self.delete_day)
                #センサ未取得検知

                #電池切れ予兆検知(異常温湿度)
                
                #処理成功をログ出力
                logging.info(f'[sucess to {self.process_name} DB [collection {collection_name}, date{str(self.masterdate)}')

            #エラー出たらログ出力
            except:
                if i == retry:
                    logging.error(f'cannot {self.process_name} DB [collection {collection_name}, date{str(self.masterdate)}, loop{str(i)}]')
                else:
                    logging.warning(f'retry to {self.process_name} DB [collection {collection_name}, date{str(self.masterdate)}, loop{str(i)}]')
                continue
            else:
                break

    ######処理実行######
    def run(self, process_name):
        #処理名を更新
        self.process_name = process_name

        #渡した処理名が一覧に含まれない場合、エラーを投げる
        if process_name not in PROCESS_NAME_LIST:
            raise ValueError('process_name is invalid')

        #設定ファイル読込
        cfg = configparser.ConfigParser()
        cfg.read('./config.ini', encoding='utf-8')

        backup_dirs = ast.literal_eval(cfg['Path']['BackupDirs'])
        log_output = cfg['Path'][f'{self.process_name}LogOutput']
        retry = int(cfg['Process']['BackupRetry'])
        user_name = cfg['DB']['UserName']
        cluster_name = cfg['DB']['ClusterName']
        db_name = cfg['DB']['DBName']
        collection_names = ast.literal_eval(cfg['DB']['CollectionNames'])
        delete_days = ast.literal_eval(cfg['Date']['DeleteDays'])

        #ログ出力ディレクトリ存在しなければ作成
        if not os.path.exists(log_output):
            os.makedirs(log_output)
        #ログの初期化
        logname = f"/atlasbackuplog_{str(self.masterdate.strftime('%y%m%d'))}.log"
        logging.basicConfig(filename=log_output + logname, level=logging.INFO)

        for k in collection_names.keys():
            collection_name = collection_names[k]
            try:
                self.backup_dir = backup_dirs[k]
                self.delete_day = delete_days[k]
            except:
                logging.error(f'keys in config.ini is invalid')
                exit()
            
            self._run_process(user_name, cluster_name, db_name, collection_name, retry)