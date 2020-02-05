# SureChEMBL mini data client

Create a SQL table of compounds from SureChEMBL database and interlink SureChEMBL IDs with compound sets

## Features
* Client can be used as a Python package or from command-line interface;
* The client was designed having Postgres in mind, however other databases should work too but with slower insertion of data;
* [surechembl-data-client](https://github.com/chembl/surechembl-data-client) can accomplish the same task but significantly slower. The client loads all data from FTP (e.g. links to publications, patent office IDs) and uses INSERT method which is slower than Postgres COPY;
* Load frontfiles for a specifc day, month or year. Default to be used to load new patent data provided by EBI daily i.e. schedule the script using crontab to run every day;
* If file directory is not found a backlog is created to load the directory on the next scheduled time;
* map_cmpd_id_surechembl_id.sql performs mapping between SureChEMBL compounds and in-house compound table (must have an InChI column) and returns interlinked compounds. Comment in/out the second snippet after UNION to enable matching while ignoring stereochemical layer.

## Dependecies
* Database account with COPY/INSERT/CREATE TABLE/ALTER TABLE privilleges;
* Python DBAPI driver for a database of your choice. Postgres (psycopg2), Oracle (cx_oracle), MySQL (mysqldb);
* Conda to create environment using environment.yml;
* Contact SureChEMBL support team for the FTP account credentials.

## Installation
```
cd surechembl_mini_client
pip install .
```
or without pip
```
python setup.py install
```

## Example usage from CLI
### Loads frontfile for a current day
```
surechembl_mini_client -fu my_ftp_user -fp my_ftp_password -du my_db_user -dp my_db_password -dh my_db_host -port my_db_port -dn my_db_name -dt my_db_type --frontfile
```
### Loads frontfile for a specific day
```
surechembl_mini_client -fu my_ftp_user -fp my_ftp_password -du my_db_user -dp my_db_password -dh my_db_host -port my_db_port -dn my_db_name -dt my_db_type --frontfile -cd 18 -cm 3 -cy 2017
```
### Loads backfile for a specific year range
```
surechembl_mini_client -fu my_ftp_user -fp my_ftp_password -du my_db_user -dp my_db_password -dh my_db_host -port my_db_port -dn my_db_name -dt my_db_type -sy 2013 -ey 2018
```

## Example usage within Python
```
from surechembl_mini_client import surechembl_mini_client
surechembl_mini_client(<arguments>)
```

## Working principle
* Connect to FTP server and get tsv directory information (can be more than one) from newfiles.txt;
* If newfiles.txt is not present look for a tsv file to parse in the same directory;
* Download tsv, parse, load to pandas and drop duplicates;
* Load to DB and drop duplicates in the database, add primary key back.

## Authors
* Written by **Aretas Gaspariunas**. Have a question? You can always ask and I can always ignore.

## Disclaimer
This client is not an official release by EBI so please use it at your own risk.
