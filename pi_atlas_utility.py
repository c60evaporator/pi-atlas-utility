from pyatlas import AtlasClient
from datetime import datetime, timedelta
import configparser
import pandas as pd
import numpy as np
import logging
from pit import Pit
import ast
import os

#処理名一覧
BACKUP_PROCESS_NAME = 'Backup'
DELETE_PROCESS_NAME = 'Delete'
BATTERY_PROCESS_NAME = 'Battery'
ACQUISITION_PROCESS_NAME = 'Acquisition'
PROCESS_NAME_LIST = [BACKUP_PROCESS_NAME, DELETE_PROCESS_NAME, BATTERY_PROCESS_NAME, ACQUISITION_PROCESS_NAME]

class PiAtlasUtility():
    #初期化
    def __init__(self, masterdate = datetime.today()):
        self.masterdate = masterdate  # 処理開始時刻
        self.backup_dir = None  # バックアップ処理時の、バックアップ先ディレクトリ
        self.process_name = None  # 実行する処理の名前
        self.delete_day = None  # 削除処理時、この日時よりも前のデータを削除する
        self.battery_collection_name = None  # 電池切れ予兆検知対象のコレクション名
        self.battery_detection_list = None  # 電池切れ予兆検知対象のセンサリスト
        self.acquisition_detection_list = None  # センサ未取得検知対象のセンサリスト

    #電池切れ予兆検知
    def _battery_anomaly_detection(self, atlasclient, collection_name):
        #予兆検知デバイスリストを走査
        for device in self.battery_detection_list.itertuples():
            #判定対象列名
            colname1 = f"no{format(device.No,'02d')}_{device.ColName1}"
            colname2 = f"no{format(device.No,'02d')}_{device.ColName2}"
            proj = {"Date_Master":1, colname1:1, colname2:1}
            #現在時刻からPeriod時間前以降の判定対象列データを取得
            startdate = self.masterdate - timedelta(hours=device.Period)
            flt = {"Date_Master":{"$gte":startdate}}
            df = atlasclient.get_collection_to_df(collection_name, filter=flt, projection=proj)
            #判定対象列1,2に関して、閾値上下限のいずれかをオーバーしたデータを取得
            if colname1 in df.columns:
                if colname2 in df.columns:
                    df_anomaly = df[(df[colname1] < device.LowerThreshold1) | (df[colname1] > device.UpperThreshold1) | (df[colname2] < device.LowerThreshold2) | (df[colname2] > device.UpperThreshold2)]
                else:
                    df_anomaly = df[(df[colname1] < device.LowerThreshold1) | (df[colname1] > device.UpperThreshold1)]
            else:
                if colname2 in df.columns:
                    df_anomaly = df[(df[colname2] < device.LowerThreshold2) | (df[colname2] > device.UpperThreshold2)]
                else:
                    logging.warning(f'colname in battery_detection_list.csv is invalid [device {device.DeviceName}, date{str(self.masterdate)}]')
                    continue

            #上下限オーバーしたデータ数がCount以上のとき、電池切れ予兆発生判定
            if len(df_anomaly) >= device.Count:
                message = f'BATTERY ANOMALY DETECTION [device {device.DeviceName}, date {str(self.masterdate)}]'
                logging.critical(message)
                #メール送信
                message = message + '\n' + str(df_anomaly.drop(['_id'], axis=1).reset_index(drop=True))
                self._send_email(message)

    #センサ未取得検知
    def _acquisition_anomaly_detection(self, atlasclient, collection_name):
        #センサ未取得検知デバイスリストから
        detection_list = self.acquisition_detection_list[self.acquisition_detection_list['CollectionName'] == collection_name]
        #現在時刻からFailureMinuteの最大分前以降の判定対象列データを取得
        max_failure_minutes = int(detection_list['FailureMinutes'].max())
        startdate = self.masterdate - timedelta(minutes=max_failure_minutes)
        flt = {"Date_Master":{"$gte":startdate}}
        df = atlasclient.get_collection_to_df(collection_name, filter=flt)
        #センサ未取得検知デバイスリストを走査
        for device in detection_list.itertuples():
            #判定対象列名
            colname = f"no{format(device.No,'02d')}_{device.ColName}"
            #FailureMinutes以降のデータのみ抽出
            device_start = self.masterdate - timedelta(minutes=device.FailureMinutes)
            df_device = df[df['Date_Master'] >= device_start]
            # 判定対象列が存在するとき、判定対象列の取得成功数をカウント
            if colname in df_device.columns:
                acquisition_num = df_device[colname].count()
            # 判定対象列が存在しない時、取得成功数を0とする
            else:
                acquisition_num = 0
            # 取得成功数=0のとき、
            if acquisition_num == 0:
                message = f'ACQUISITION FAILURE DETECTION [device {device.DeviceName}, date {str(self.masterdate)}]'
                self._send_email(message)
            
    # 検知ログの作成
    def _make_detection_log(self):
        print('')

    #メール送信
    def _send_email(self, message):
        print(message)

    #処理本体を実行
    def _run_process(self, user_name, cluster_name, db_name, collection_name, retry):
        pa = Pit.get('atlas')[':pa']
        num = Pit.get('atlas')[':num']
        pad = ''.join([chr(ord(a) + num + 5) for a in pa])
        for i in range(retry):
            try:
                atlasclient = AtlasClient(user_name=user_name, cluster_name=cluster_name, db_name=db_name, password=pad)
                #バックアップ処理
                if self.process_name == BACKUP_PROCESS_NAME:
                    atlasclient.backup_previous_month(collection_name, "Date_Master", datetime.now(), self.backup_dir)
                #一定日以上前のデータを削除
                elif self.process_name == DELETE_PROCESS_NAME:
                    atlasclient.delete_previous_data(collection_name, "Date_Master", self.delete_day)
                #電池切れ予兆検知(異常温湿度)
                elif self.process_name == BATTERY_PROCESS_NAME and collection_name == self.battery_collection_name:
                    self._battery_anomaly_detection(atlasclient, collection_name)
                #センサ未取得検知
                elif self.process_name == ACQUISITION_PROCESS_NAME and collection_name in self.acquisition_detection_list['CollectionName'].unique().tolist():
                    self._acquisition_anomaly_detection(atlasclient, collection_name)
                
                #処理成功をログ出力
                logging.info(f'sucess to {self.process_name} DB [collection {collection_name}, date{str(self.masterdate)}')

            #エラー出たらログ出力
            except:
                if i == retry - 1:
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
        self.battery_collection_name = cfg['DB']['BatteryCollectionName']
        delete_days = ast.literal_eval(cfg['Date']['DeleteDays'])

        #電池切れ予兆検知リスト読込
        self.battery_detection_list = pd.read_csv('./battery_detection_list.csv')

        #センサ未取得検知リスト読込
        self.acquisition_detection_list = pd.read_csv('./acquisition_detection_list.csv')

        #ログ出力ディレクトリ存在しなければ作成
        if not os.path.exists(log_output):
            os.makedirs(log_output)
        #ログの初期化
        logname = f"/Atlas{self.process_name}Log_{str(self.masterdate.strftime('%y%m%d'))}.log"
        logging.basicConfig(filename=log_output + logname, level=logging.INFO)

        #コレクション(テーブル)を走査
        for k in collection_names.keys():
            collection_name = collection_names[k]
            try:
                self.backup_dir = backup_dirs[k]
                self.delete_day = delete_days[k]
            except:
                logging.error(f'keys in config.ini is invalid')
                exit()
            
            self._run_process(user_name, cluster_name, db_name, collection_name, retry)