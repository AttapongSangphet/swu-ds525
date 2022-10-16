# Data Modeling II - Building a Data Warehouse

## First, We create AWS S3 as our datalake that we can store our JSON files, Events data and JSON Path.
### AWS Redshift
![AWS S3](pictures/pic04.jpg)

## AWS Redshift
![AWS Redshift](pictures/pic01.jpg)


## Then, We create AWS Redshift, an AWS data warehouse, for loading data from S3 into it 

### Create AWS Redshift
![AWS Redshift](pictures/pic01.jpg)


## Now we can use etl scripts for running query command whatever we want
## Getting Started
```sh
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Running ETL Script
```sh
python etl.py
```




