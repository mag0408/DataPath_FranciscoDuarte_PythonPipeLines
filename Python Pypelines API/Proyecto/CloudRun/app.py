{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "010b9cfc-d676-4ee2-b726-bfd6f5b56e99",
   "metadata": {},
   "outputs": [],
   "source": [
    "import os\n",
    "from flask import Flask, request, jsonify\n",
    "from google.cloud import bigquery\n",
    "\n",
    "app = Flask(__name__)\n",
    "\n",
    "@app.route(\"/trigger-bigquery-load\", methods=[\"POST\"])\n",
    "def trigger_bigquery_load(table):\n",
    "    try:\n",
    "        # Read configuration\n",
    "        table_id = f\"ci-sandbox-data.classicmodels.{table}\"\n",
    "        gcs_uri = f\"gs://gcp_api_etl_rpoject/{table}.csv\"\n",
    "\n",
    "        # Initialize BigQuery client\n",
    "        client = bigquery.Client()\n",
    "\n",
    "        # Configure the BigQuery Load Job\n",
    "        job_config = bigquery.LoadJobConfig(\n",
    "            schema=None,  # Auto-detect schema from CSV\n",
    "            skip_leading_rows=1,\n",
    "            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,\n",
    "            source_format=bigquery.SourceFormat.CSV,\n",
    "        )\n",
    "        \n",
    "        # Sets file delimiter\n",
    "        job_config.field_delimiter = \",\"\n",
    "\n",
    "        # Load data from GCS to BigQuery\n",
    "        load_job = client.load_table_from_uri(\n",
    "            gcs_uri, table_id, job_config=job_config\n",
    "        )\n",
    "\n",
    "        # Wait for the job to complete\n",
    "        load_job.result()\n",
    "\n",
    "        return \"BigQuery load job completed successfully.\", 200\n",
    "    except Exception as e:\n",
    "        return str(e), 500\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    app.run(host=\"0.0.0.0\", port=8080)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
