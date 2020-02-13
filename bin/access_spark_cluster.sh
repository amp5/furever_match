# copy local files into S3 bucket for spark master to access
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/import_petfinder.py s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/postgres_setup/postgresql-42.2.9.jar s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/connect_to_s3.py s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/process_petfinder.py s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/write_to_postgres.py s3://fureverdump/


# log into SSH
ssh -i "Alexandra-Plassaras-IAM-keypair.pem" ubuntu@ec2-54-149-213-136.us-west-2.compute.amazonaws.com


###### NOT Working ######
# copy files from s3 bucket to master machine
aws s3 cp s3://fureverdump/import_petfinder.py .
aws s3 cp s3://fureverdump/postgresql-42.2.9.jar .
aws s3 cp s3://fureverdump/connect_to_s3.py .
aws s3 cp s3://fureverdump/process_petfinder.py .
aws s3 cp s3://fureverdump/write_to_postgres.py .


###### NOT Working ######
spark-submit --master yarn --deploy-mode client --jars /postgresql-42.2.9.jar connect_to_s3.py