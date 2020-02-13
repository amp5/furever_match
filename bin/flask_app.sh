# copy local files into S3 bucket for spark master to access
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/access_db.py s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/server.py s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/templates/base.html s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/templates/dashboard.html s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/templates/data_explore.html s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/templates/data_overview.html s3://fureverdump/
aws s3 cp /Users/alexandraplassaras/src/furever_match/src/templates/missing_data.html s3://fureverdump/


# log into SSH
ssh -i "Alexandra-Plassaras-IAM-keypair.pem" ubuntu@$ec2-54-185-33-148.us-west-2.compute.amazonaws.com


###### NOT Working ######
# copy files from s3 bucket to master machine
aws s3 cp s3://fureverdump/access_db.py .templates/
aws s3 cp s3://fureverdump/server.py .templates/
aws s3 cp s3://fureverdump/base.html .templates/
aws s3 cp s3://fureverdump/dashboard.html .templates/
aws s3 cp s3://fureverdump/data_explore.html .templates/
aws s3 cp s3://fureverdump/data_overview.html .templates/
aws s3 cp s3://fureverdump/missing_data.html .templates/
