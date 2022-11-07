# Building a Data Lake

## getting start

### change to directory of your prepared scripts

```sh
cd 04-build-a-data-lake/
```

### running docker-compose of pyspark-notebook service (On Gitpod)

```sh
docker-compose up
```

### Then we can follow url address of port 8888 for running ETL scipts on pyspark-notebook

![pyspark-notebook address](pictures/ports_address.jpg)

![pyspark-notebook](pictures/pyspark-notebook_window.jpg)

### running ETL scripts to create new data tables from JSON files

![Actors table](pictures/actors.jpg)
![Repos table](pictures/repos.jpg)
![Orgs table](pictures/orgs.jpg)
![Events table](pictures/events.jpg)

### we can save created tables to CSV or parquet file as  shown in the picture below

![save to cvs](pictures/saved_csv_files.jpg)

### or transfrom your data to any table you want

![Example data transformation](pictures/query_result.jpg)
