# Data Modeling II - Building a Data Warehouse

## First, We create AWS S3 as our datalake that we can store our JSON files, Events data and JSON Path.
### Create AWS S3
![AWS S3](pictures/pic04.jpg)

## Then, We create AWS Redshift, an AWS data warehouse, for loading data from S3 into it.

### Create AWS Redshift
![AWS Redshift](pictures/pic01.jpg)

### enable publicly asccessible of AWS Redshift
![Modify publicly accessible setting](pictures/pic03.jpg)

### Configure inbound rules of IAM to make sure that we are allowed to access to AWS Redshift, specific soreces an types is recommended.
![IAM-Inbound rules](pictures/pic07.jpg)

### Change paths of AWS S3, AWS Redshift and IAM roles to the correct paths that we need to connect before running ETL Script.
### S3 Connection path
![S3 Connection path](pictures/pic10.jpg)
### Redshift Connection path
![Redshift Connection path](pictures/pic11.jpg)

## Now we can access to AWS Redshift by running provided etl scripts of query commands.

### Getting Started
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

### Running ETL Script
```sh
python etl.py
```

## Data Model
![data model](pictures/pic08.jpg)

## Some examples of data query are shown in pictures below
### Events table
![Events table](pictures/pic08.jpg)
### Events table (by Query Editor in AWS Redshift)
![Events table (by Query Editor in AWS Redshift)](pictures/pic09.jpg)




