#各種Pythonモジュールのインポート
import datetime
import os

import airflow
from airflow.contrib.operators import bigquery_operator, \
    bigquery_table_delete_operator, gcs_to_bq
import pendulum

# リスト6-2. DAG内のオペレータ共通のパラメータの定義
# DAG内のオペレータ共通のパラメータを定義する。
# depends_on_pastは前のタスクが成功したかどうかで、次のタスクを実行するかどうかを決めるパラメータ
# retriesは失敗したタスクをその後何回りトライするかを表す
# retry_delayはリトライを失敗してからどのぐらい後に実行するかを表す
# start_dateはこのタスクが実行される時間を開始日に設定する
# この項目があるため実務において確認（提案）すべきこととして、あるワークフローを作りたいときに、大きく①タスクが失敗した時の処理はどうするか②いつ実行するかを決めれる
# 失敗した時に、メール送るか送るなら宛先、リトライするか、リトライするなら何回、いつ行うかを決めれる
# 宛先はまとめれる？
default_args = {
    'owner': 'data-engineer-5125-336206',
    'depends_on_past': True,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    # DAG作成日の午前2時(JST)を開始日時とする。
    'start_date': pendulum.today('Asia/Tokyo').add(hours=2)
}

# リスト6-3. DAGの定義
# DAGを定義する。
with airflow.DAG(
        'etl_service_industry',
        default_args=default_args,
        # 日次でDAGを実行する。
        schedule_interval=datetime.timedelta(days=1),
        catchup=False) as dag:
    # リスト6-4. ユーザ行動ログ取り込みタスクの定義
    # Cloud Storage上のユーザ行動ログをBigQueryの作業用テーブルへ
    # 取り込むタスクを定義する。
    load_events = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_events',
        bucket='data-engineer-5125-336206',
        #source_objects=['data/events/{{ ds_nodash }}/*.json.gz'],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_objects=['service_industry_sales.csv'],
        destination_project_dataset_table='workflow_test.SI_raw',
        source_format='CSV',
        autodetect=True
    )
    # リスト6-5. gcpbook_ch5.dauテーブルへの書き込みタスクの定義
    # BigQueryの作業用テーブルとユーザ情報テーブルを結合し、課金ユーザと
    # 無課金ユーザそれぞれのユーザ数を算出して、結果をgcpbook_ch5.dau
    # テーブルへ書き込むタスクを定義する。
    etl = bigquery_operator.BigQueryOperator(
        task_id='etl',
        use_legacy_sql=False,
        sql="""
        #temp1はカラム名を変更&YYYY年MMorM年DDになっているもののみ抽出(deteでpがSI_rawついているのもを除去)
        CREATE OR REPLACE TABLE  `data-engineer-5125-336206.workflow_test.SI_raw_aferTransaction`AS
        (WITH `temp1` AS 
        (SELECT
            ____________ AS item
            ,_____________________ AS industry
            ,______ AS area
            ,CASE WHEN ____________________________________ LIKE '%p' THEN REPLACE(____________________________________,'  p','') 
            ELSE ____________________________________ END AS date
            ,unit
            ,value
        FROM `data-engineer-5125-336206.workflow_test.SI_raw`
        WHERE REGEXP_CONTAINS( ____________________________________,'.*年.月|.*年..月')
        )
        ,`temp2` AS  #temp2は年を-0or-に変更
        (SELECT 
            CASE WHEN date LIKE '%p' THEN REPLACE(date,'  p','') 
                WHEN date LIKE '%年_月' THEN REPLACE(date,'年','-0')
                WHEN date LIKE '%年__月' THEN REPLACE(date,'年','-') 
                ELSE date
            END AS date
            ,item
            ,industry
            ,area
            ,value
            unit
        FROM `temp1`
        )
        #temp2の月を-01に変更して抽出
        SELECT 
          REPLACE(date,'月','-01') AS date
          ,item
          ,industry
          ,area
          ,value
          ,unit
          FROM `temp2`
          ORDER BY industry ,date ASC)
        """
    )

    # リスト6-6. 作業用テーブルを削除するタスクの定義
    # BigQueryの作業用テーブルを削除するタスクを定義する。
    delete_work_table = \
        bigquery_table_delete_operator.BigQueryTableDeleteOperator(
            task_id='delete_work_table',
            deletion_dataset_table='workflow_test.SI_raw'
        )

    # リスト6-7. タスクの依存関係の定義
    # 各タスクの依存関係を定義する。
    load_events >> etl >> delete_work_table