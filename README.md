# Task 2 (Technical Test Data Engineering)

## Desc
This program is implement ETL concept (Extract, Load, Transform) to extract data (movie, rating) from MySQL database, transform data (cleaning and aggregate), and load transformed data into database. This program use Python programming language.

## How to Run
a.	Install Requirements
```
python -m pip install -r requirements.txt
```
But, if you want use virtual environment:
```
python -m venv your_name_env
```
```
source path/to/your_name_env/bin/activate
```
```
python -m pip install -r requirements.txt
```

b.	Setting Variables
First open file main.py then search and adjust this variable below:

```
conn = etl.connection(
        	username='your_db_username', 
        	password='your_db_password', 
        	host='your_db_host', 
        	port=your_db_port, 
        	database=your_db_name', 
        	database_type='mysql+pymysql'
)
```

c.	Run
```
python main.py
```


