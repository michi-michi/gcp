def industry_sales_extract(request):
    import requests
    import json
    import csv
    from google.cloud import storage as gcs
    project_id = "data-engineer-5125-336206"
    client = gcs.Client(project_id)
    #バケットのインスタンスを取得
    bucket = client.bucket('data-engineer-5125-336206')
    #ファイルのblobインスタンスを取得
    blob = bucket.blob('service_industry_sales.csv')
    url = "http://api.e-stat.go.jp/rest/3.0/app/getSimpleStatsData"
    payload = {"cdTab":"001","cdArea":"00000","appId":"350f94b10f4d25be2337784739e3f596d6b1beb1","lang":"J", "statsDataId":"0003179100","metaGetFlg":"N","cntGetFlg":"N","explanationGetFlg":"N","annotationGetFlg":"N","replaceSpChars":"2","sectionHeaderFlg":"2"}
    r = requests.get(url, params=payload)
    blob.upload_from_string(data=r.text, content_type="text/csv")
    return print("success")