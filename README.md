Have a bucket create, say test-bucket-eh
Sources:
https://cloud.google.com/dataproc/docs/guides/create-cluster
https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example
https://cloud.google.com/dataproc/docs/quickstarts/quickstart-lib
https://cloud.google.com/dataproc/docs/concepts/versioning/overview

python3 -m venv .venvs/test_env
source .venvs/test_env/bin/activate
python3 -m pip install google-cloud-dataproc
python3 -m pip install --upgrade google-cloud-storage
cd spark_test

#Option 1:
#Separetly create a cluster and send spark job through the console
python3 master.py (with the create cluster uncommented)
gcloud dataproc jobs submit pyspark wordcount.py --cluster "temp-cluster" --region "us-east1" --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.11.jar
##Gives a no permission error. Not sure why.

#Option 2:
#Use a script to create a cluster, submit the job and then delete the cluster.
gsutil cp wordcount.py gs://test-bucket-eh/spark_test/
python quickstart.py --cluster_name "temp-cluster" --region "us-east1" --project_id "eh-pyspark" --job_file_path gs://test-bucket-eh/spark_test/wordcount.py
#It is working!

##Setup package stuff:
https://python-packaging.readthedocs.io/en/latest/minimal.html
pip install .
from spark_test import quickstart
quickstart.quickstart_func()
