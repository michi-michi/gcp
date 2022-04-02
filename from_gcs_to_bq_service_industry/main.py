def transaction_SI_raw(request):
  from google.cloud import bigquery
  from google.cloud import storage as gcs
  client = bigquery.Client()
  # TODO(developer): Set table_id to the ID of the table to create.
  # table_id = "your-project.your_dataset.your_table_name"
  job_config = bigquery.LoadJobConfig(
    autodetect=1,
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
  )
  uri = "gs://data-engineer-5125-340406/service_industry_sales.csv"
  table_id = "data-engineer-5125.workflow_test"
  load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
  )  # Make an API request.
  load_job.result()  # Waits for the job to complete.
  destination_table = client.get_table(table_id)  # Make an API request.
  print("Loaded {} rows.".format(destination_table.num_rows))