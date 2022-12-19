# Capstone Project Construction

### This project provide 

### Raw data that I chose for this project is the information about UK's housing price paid from Kaggle website. It is available to download [here](https://www.kaggle.com/datasets/hm-land-registry/uk-housing-prices-paid).


## Data Modeling

![Data Modeling](pictures/data_modeling.jpg)

## Create AWS S3 Bucket

### First of all, we need to create the AWS S3 bucket as our "data lake" to collect raw data.
### In this step, S3 bucket must be eited "block public access" and bucket policy to allow all access from public can connect to the bucket (note: this edition is not recemmended for practical use).

### To get AWS Credential keys for connecting to S3 bucket, we can use code below on the AWS interface as shown in picture to get "aws_access_key", "aws_secret_access_key", "aws_session_token"

```sh
cat ~/.aws/credentials
```
![AWS credentials](pictures/AWS_S3_Credentials_edited.jpg)

### and we can find AWS S3 URI in the S3 properties interface as shown in pictue below

![AWS S3 URI](pictures/AWS_S3_URI_edited.jpg)

## Loading raw data to Datalake, AWS S3 bucket
### To connect to AWS S3 bucket, AWS Credential keys as mentioned above and S3 URI are required in this step. 
### Change connecting configurations in etl code following by AWS Credential keys and S3 URI.

### Then create and activate virtual environment and install tools as provided in requirements file.

```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Run docker-compose 
### our docker 
```sh
docker-compose up
```

### run provided etl code to load raw data to AWS s3 Bucket.

```sh
python etl_load_data_to_s3.py
```

### Now, our data was uploaded to S3 bucket

![AWS S3](pictures/AWS_S3.jpg)


## Create AWS Redshift
### AWS Redshift will be used as data warehouse for tranfroming raw data from data lake, AWS S3, to another tables that we need in AWS Redshift.

### Same as the connection of AWS S3, We must use specific endpoint of Redshift cluster, password, database name, username and port number for connecting to AWS Redshift.
![AWS Redshift Endpoint](pictures/AWS_Redshift_edited.jpg)


## ETL PySpark with S3


