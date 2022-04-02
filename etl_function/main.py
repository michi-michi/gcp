def transaction_SI_raw(request):
  from google.cloud import bigquery as bq
  from google.cloud import storage as gcs
  client = bq.Client()
  query = """
    create or replace table  `data-engineer5125.workflow_test.SI_raw_aferTransaction` as
      (with `temp1` as 
        (select
            ____________ as item
            ,_____________________ as industry
            ,______ as area
            ,case when ____________________________________ like '%p' then replace(____________________________________,'  p','') 
            else ____________________________________ end as date
            ,unit
            ,value
        from `data-engineer5125.SI_analysys.SI_raw`
        where regrep_contains( ____________________________________,'.*年.月|.*年..月')
        )
      ,`temp2` as  #temp2は年を-0or-に変更
        (select 
          case when date LIKE '%p' then replace(date,'  p','') 
              when date LIKE '%年_月' then replace(date,'年','-0')
              when date LIKE '%年__月' then replace(date,'年','-') 
              ELSE date
          end as date
          ,item
          ,industry
          ,area
          ,value
          unit
        FROM `temp1`
        )
        #temp2の月を-01に変更して抽出
        select 
          replace(date,'月','-01') AS date
          ,item
          ,industry
          ,area
          ,value
          ,unit
          from `temp2`
          order by industry ,date asc)
  """
  query_job = client.query(query)
  print("The query data:")