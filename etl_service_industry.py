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
    'owner': 'data-engineer-5125-340406',
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
        bucket='data-engineer-5125-340406',
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
    transaction_1 = bigquery_operator.BigQueryOperator(
        task_id='transaction_1',
        use_legacy_sql=False,
        sql="""
        CREATE OR REPLACE TABLE  `data-engineer-5125-340406.workflow_test.SI_raw_aferTransaction`AS
        (WITH `temp1` AS 
        (SELECT
            ____________ AS item
            ,_____________________ AS industry
            ,______ AS area
            ,CASE WHEN ____________________________________ LIKE '%p' THEN REPLACE(____________________________________,'  p','') 
            ELSE ____________________________________ END AS date
            ,unit
            ,value
        FROM `data-engineer-5125-340406.workflow_test.SI_raw`
        WHERE REGEXP_CONTAINS( ____________________________________,'.*年.月|.*年..月')
        )
        ,`temp2` AS
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
            ,unit
        FROM `temp1`
        )
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
    transaction_2 = bigquery_operator.BigQueryOperator(
        task_id='transaction_2',
        use_legacy_sql=False,
        sql="""
        create or replace table `data-engineer-5125-340406.workflow_test.category_add_service_industry_sales` as( 
            with middle_category_add_tbl as(
                select
                  date
                  ,item
                  ,case when industry in('その他') then 'その他'
                  else regexp_extract(industry,'^..(.*)') 
                  end as middle_category_name
                  ,area
                  ,cast(case when regexp_extract(industry,r'^\\d{1,2}') = '4' then '46'
                  else regexp_extract(industry,r'^\\d{1,2}')
                  end as int)  as middle_category  
                  ,value
                  ,unit
                from 
                    `data-engineer-5125-340406.workflow_test.SI_raw_aferTransaction`
                where 
                    industry not like '82aうち社会教育，職業・教育支援施設'
                and industry not like '82bうち学習塾，教養・技能教授業'
                order by 3)
            
            ,large_category_add_tbl as(
                select
                  date
                  ,item
                  ,middle_category_name
                  ,area
                  ,case
                  when middle_category between 37 and 41 then 'G'
                  when middle_category between 42 and 49 then 'H'
                  when middle_category between 68 and 70 then 'K'
                  when middle_category between 72 and 74 then 'L'
                  when middle_category between 75 and 77 then 'M'
                  when middle_category between 78 and 80 then 'N'
                  when middle_category = 82 then 'O'
                  when middle_category between 83 and 85 then 'P'
                  when middle_category between 88 and 95 then 'R'
                  else null
                  end as large_category
                  ,middle_category
                  ,value
                  ,unit
                from
                  middle_category_add_tbl
                where
                  middle_category is not null
                or
                  middle_category_name in ('その他'))
            select 
              date
              ,item
              ,area
              ,middle_category_name
              ,large_category
              ,middle_category
              ,value
              ,unit
            from
              large_category_add_tbl
            order by middle_category asc)
        """
    )
    transaction_3 = bigquery_operator.BigQueryOperator(
        task_id='transaction_3',
        use_legacy_sql=False,
        sql="""
        create or replace table `data-engineer-5125-340406.workflow_test.large_category_table` as(
            select 
                normalize(regexp_extract(industry,'.'),NFKC) as large_category
                ,regexp_extract(industry,'^.(.*)') as large_category_name
            from 
                `data-engineer-5125-340406.workflow_test.SI_raw_aferTransaction`
            where 
                regexp_extract(industry,r'^\\d{1,2}') is null
            and 
                industry not in ('合計','サービス産業計','その他')
            group by 1,2)
        """
    )
    transaction_4 = bigquery_operator.BigQueryOperator(
        task_id='transaction_4',
        use_legacy_sql=False,
        sql="""
        create or replace table `data-engineer-5125-340406.workflow_test.service_industry_mart` as(
            select 
                date
                ,item
                ,area
                ,category_add_service_industry_sales.large_category
                ,large_category_name
                ,middle_category
                ,middle_category_name
                ,value
                ,unit
            from 
                `data-engineer-5125-340406.workflow_test.category_add_service_industry_sales` as category_add_service_industry_sales
            left join `data-engineer-5125-340406.workflow_test.large_category_table` as large_category_tbl
                on category_add_service_industry_sales.large_category = large_category_tbl.large_category
            order by 6,1)
        """
    )
    # リスト6-6. 作業用テーブルを削除するタスクの定義
    # BigQueryの作業用テーブルを削除するタスクを定義する。
    delete_work_table = \
        bigquery_table_delete_operator.BigQueryTableDeleteOperator(
            task_id='delete_work_table',
            deletion_dataset_table='data-engineer-5125-340406.workflow_test.SI_raw'
        )
    # リスト6-7. タスクの依存関係の定義
    # 各タスクの依存関係を定義する。
    load_events >> transaction_1 >> transaction_2 >>transaction_3 >> transaction_4 >> delete_work_table