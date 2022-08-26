# Data Modeling I

## Getting Started

```sh
cd 01-data-modeling-i
python -m venv ENV
source ENV/bin/activate
pip install -r requirements.txt
```

## Running Postgres

```sh
docker-compose up
```

To shutdown, press Ctrl+C and run:

```sh
docker-compose down
```

## Running Create_table Script
```sh
python create_table.py
```

## Running etl Script
```sh
python etl.py
```
