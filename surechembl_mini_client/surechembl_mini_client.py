#!/usr/bin/env python

# Author: Aretas Gaspariunas

# todo: parallelize backfile and frontfile functions, tests

import sys
import os
import logging
import ftplib
import gzip
import datetime
from typing import Optional, List, Dict

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, exc, MetaData, Table, Column, Integer, String, Text
from sqlalchemy.engine.base import Engine
import pandas as pd
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
try:
    import psycopg2
except ImportError:
    psycopg2 = None
try:
    import MySQLdb
except ImportError:
    MySQLdb = None

class AppLogger:

    @classmethod
    def get(cls, name: str, log_file: str, file_level=logging.INFO,
        stream_level=logging.WARNING) -> logging.Logger:

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s : %(levelname)s : %(name)s : %(funcName)s :%(message)s"
        )

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(stream_level)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        return logger

def count_rows(engine: Engine, tbl_name: str) -> int:

    if not engine.dialect.has_table(engine, tbl_name):
        logger.error('Destination table does not exist.')
        sys.exit()

    return int(engine.execute("""SELECT count(*) FROM "{0}" """.format(tbl_name)).fetchone()[0])

def ftp_connect(ftp_usr: str, ftp_psw: str, ftp_address: Optional[str]='ftp-private.ebi.ac.uk'):
    # connecting to FTP server
    try:
        ftp = ftplib.FTP(ftp_address, ftp_usr, ftp_psw)
        return ftp
    except Exception as e:
        logger.error('Failed to connect to FTP server. Please check connection details.\n{}'.format(e))
        raise

def parse_chemicals_file(tsv_path: str, unique_col: List[str]) -> pd.DataFrame:

    try:
        with gzip.open(tsv_path) as f:
            df = pd.read_csv(f, sep='\t')
    except Exception as e:
        os.remove(tsv_path)
        logger.error('Failed to open and read {}.'.format(tsv_path))
        raise e

    df = df[['SureChEMBL ID','SMILES','Standard InChi','Standard InChiKey']]
    df.columns = ['schembl_chem_id', 'smiles', 'std_inchi', 'std_inchikey']
    df.drop_duplicates(subset=unique_col, keep='first', inplace=True)

    return df

def get_tsv_dir(ftp: ftplib.FTP) -> Dict[str, str]:

    dir_dict = {}

    # checking if tsv exists in dir
    frontfile_dir = ftp.pwd()
    f_list = ftp.nlst()
    tsv_list = [i for i in f_list if i.endswith('.chemicals.tsv.gz')]
    # checking if newfiles.txt exists
    newfiles_list = [i for i in f_list if i == 'newfiles.txt']
    if len(newfiles_list) > 1:
        logger.error("More than one newfiles.txt for '{0}'. Terminating.".format(frontfile_dir))
        sys.exit()

    if newfiles_list:
        logger.debug('Using newfile.txt to find records.')
        # open new file, find dir
        with open('newfiles.txt', 'wb') as newfile:
            ftp.retrbinary('RETR ' + 'newfiles.txt', newfile.write)

        with open('newfiles.txt', 'r') as newfile:
            for line in newfile:
                if 'chemicals' in line and not 'supp' in line:
                    tsv_dir, tsv = os.path.split(line.rstrip('\n'))
                    dir_dict[tsv_dir] = tsv
            if not dir_dict:
                logger.info("newfiles.txt did not contain directory information for '{0}'.".format(frontfile_dir))

        os.remove('newfiles.txt')

    elif tsv_list:
        logger.warning("Did not find newfiles.txt for '{0}'. Using .tsv file.".format(frontfile_dir))
        for i in tsv_list:
            dir_dict[frontfile_dir] = i
    else:
        logger.warning('Did not find newfiles.txt or .tsv files for today. Please investigate. Terminating.')

    return dir_dict

def get_frontfile_df(
    dir_dict: Dict[str, str],
    ftp: ftplib.FTP,
    unique_col: List[str]) -> pd.DataFrame:

    parent_dir = ftp.pwd()
    # retrieving new compounds
    frontfile_df = pd.DataFrame()
    for tsv_dir, tsv in dir_dict.items():
        ftp.cwd('data/external/frontfile' + tsv_dir)

        with open(str(tsv), 'wb') as frontfile:
            ftp.retrbinary('RETR ' + str(tsv), frontfile.write)

        # parsing the frontfile
        df = parse_chemicals_file(tsv, unique_col)
        os.remove(str(tsv))
        ftp.cwd(parent_dir)
        frontfile_df = pd.concat([frontfile_df, df])

    frontfile_df.drop_duplicates(subset=unique_col, keep='first', inplace=True)

    return frontfile_df

def load_backfile(
    engine: Engine,
    unique_col: List[str],
    logger: logging.Logger,
    ftp_usr: str,
    ftp_psw: str,
    start_year: Optional[int]=1950,
    end_year: Optional[int]=2018
    ) -> None:

    '''
    Loads backfiles for a specified range in years.
    '''

    # connecting to FTP server
    ftp = ftp_connect(ftp_usr, ftp_psw)

    parent_dir = ftp.pwd()
    backfile_dir = 'data/external/backfile'
    ftp.cwd(backfile_dir)
    year_list = ftp.nlst()

    # dropping primary key
    try:
        engine.execute("""ALTER TABLE "{0}" DROP CONSTRAINT "{0}_pkey" """.format(tbl_name))
    except Exception as e:
        logger.info('Failed to drop PK constraint.\n{}'.format(e))

    # iterating every year in the list
    for year in year_list:

        # only reads years in specified range
        if int(year.split('_')[0]) >= start_year and int(year.split('_')[0]) <= end_year:
            pass
        else:
            continue

        logger.info('Attempting to access folder for year {}.'.format(year))
        year_df = pd.DataFrame()

        ftp = ftp_connect(ftp_usr, ftp_psw)

        ftp.cwd(parent_dir)
        ftp.cwd(os.path.join(backfile_dir, year))

        if not ftp.nlst():
            logger.info('Directory for year {} is empty. Skipping.'.format(year))
            continue

        for tsv in ftp.nlst():

            if not tsv.endswith('.tsv.gz'):
                continue

            logger.info('Downloading {}'.format(tsv))
            with open(str(tsv), 'wb') as backfile:
                ftp.retrbinary('RETR ' + str(tsv), backfile.write)

            # parsing the backfile
            backfile_df = parse_chemicals_file(tsv, unique_col)
            os.remove(str(tsv))

            # writting backfile to the databases
            logger.info('Loading {} data to dataframe.'.format(tsv))
            year_df = pd.concat([year_df, backfile_df])

        ftp.quit()

        if year_df.empty:
            continue

        year_df.drop_duplicates(subset=unique_col, keep='first', inplace=True)

        dfloader(year_df, engine, tbl_name, unique_col=unique_col)
        logger.info('Finished loading {}.'.format(year))

    logger.info('Adding primary key.')
    try:
        engine.execute("""ALTER TABLE "{0}" ADD PRIMARY KEY ("{1}")""".format(tbl_name, unique_col[0]))
    except:
        pass

def load_backfile2(
    engine: Engine,
    unique_col: List[str],
    logger: logging.Logger,
    ftp_usr: str,
    ftp_psw: str,
    start_year: Optional[int]=1950,
    end_year: Optional[int]=2018
    ) -> None:

    '''
    Loads backfiles for a specified range in years.
    '''

    # connecting to FTP server
    ftp = ftp_connect(ftp_usr, ftp_psw)

    parent_dir = ftp.pwd()
    backfile_dir = 'data/external/backfile'
    ftp.cwd(backfile_dir)
    year_list = ftp.nlst()

    # dropping primary key
    try:
        engine.execute("""ALTER TABLE "{0}" DROP CONSTRAINT "{0}_pkey" """.format(tbl_name))
    except Exception as e:
        logger.info('Failed to drop PK constraint.\n{}'.format(e))

    # iterating every year in the list
    for year in year_list:

        # only reads years in specified range
        if int(year.split('_')[0]) >= start_year and int(year.split('_')[0]) <= end_year:
            pass
        else:
            continue

        logger.info('Attempting to access folder for year {}.'.format(year))
        year_df = pd.DataFrame()

        try:
            noop_resp = ftp.voidcmd('NOOP')
        except Exception:
            ftp = ftp_connect(ftp_usr, ftp_psw)

        ftp.cwd(parent_dir)
        ftp.cwd(os.path.join(backfile_dir, year))

        if not ftp.nlst():
            logger.info('Directory for year {} is empty. Skipping.'.format(year))
            continue

        def backfile_to_df(tsv, ftp=ftp):

            if not tsv.endswith('.tsv.gz'):
                return pd.DataFrame()

            logger.info('Downloading {}'.format(tsv))
            with open(str(tsv), 'wb') as backfile:
                ftp.retrbinary('RETR ' + str(tsv), backfile.write)

            # parsing the backfile
            backfile_df = parse_chemicals_file(tsv, unique_col)
            os.remove(str(tsv))

            # writting backfile to the databases
            logger.info('Loading {} data to dataframe.'.format(tsv))

        from multiprocessing import Pool, cpu_count
        pool = Pool(cpu_count())
        results = pool.map(backfile_to_df, ftp.nlst())
        pool.close()  # 'TERM'
        pool.join()   # 'KILL'
        year_df = pd.concat(results)

        # for tsv in ftp.nlst():

        #     if not tsv.endswith('.tsv.gz'):
        #         continue

        #     logger.info('Downloading {}'.format(tsv))
        #     with open(str(tsv), 'wb') as backfile:
        #         ftp.retrbinary('RETR ' + str(tsv), backfile.write)

        #     # parsing the backfile
        #     backfile_df = parse_chemicals_file(tsv, unique_col)
        #     os.remove(str(tsv))

        #     # writting backfile to the databases
        #     logger.info('Loading {} data to dataframe.'.format(tsv))
        #     year_df = pd.concat([year_df, backfile_df])

        ftp.quit()

        if year_df.empty:
            continue

        year_df.drop_duplicates(subset=unique_col, keep='first', inplace=True)

        dfloader(year_df, engine, unique_col=unique_col)
        logger.info('Finished loading {}.'.format(year))

    logger.info('Adding primary key.')
    engine.execute("""ALTER TABLE "{0}" ADD PRIMARY KEY ("{1}")""".format(tbl_name, unique_col[0]))

def load_frontfile(
    engine: Engine,
    unique_col: List[str],
    logger: logging.Logger,
    ftp_usr: str,
    ftp_psw: str,
    custom_day: Optional[int]=None,
    custom_month: Optional[int]=None,
    custom_year: Optional[int]=None
    ) -> None:

    '''
    Fetches frontfiles for a specific date or date range.
    Default behaviour - if no custom date is provided records for today are fetched.
    '''

    # connecting to FTP server
    ftp = ftp_connect(ftp_usr, ftp_psw)

    parent_dir = ftp.pwd()
    path = os.path.dirname(os.path.abspath(__file__))

    # finding dir paths for specified time
    frontfile_dir_list = []
    if custom_day is None and custom_month is None and custom_year is None:
        # today mode
        today = datetime.datetime.now()
        year, month, day = [str(today.year), str(today.month).zfill(2), str(today.day).zfill(2)]
        frontfile_dir = 'data/external/frontfile/{}/{}/{}'.format(year, month, day)
        frontfile_dir_list = [frontfile_dir]
        # reading backlog
        if os.path.isfile(os.path.join(path,'schembl_backlog.txt')):
            with open(os.path.join(path,'schembl_backlog.txt'), 'r') as f:
                for line in f:
                    frontfile_dir_list.append(line)
            os.remove(os.path.join(path,'schembl_backlog.txt'))
    elif custom_day is None and custom_month is None and custom_year is not None:
        # year mode
        year, month, day = [str(custom_year), None, None]
        frontfile_dir = 'data/external/frontfile/{}'.format(year)
        ftp.cwd(frontfile_dir)
        for month in ftp.nlst():
            ftp.cwd(parent_dir)
            ftp.cwd(frontfile_dir + '/' + month)
            for day in ftp.nlst():
                frontfile_dir_list.append(frontfile_dir + '/' + month + '/' + day)
    elif custom_day is None and custom_month is not None and custom_year is not None:
        # month mode
        year, month, day = [str(custom_year), str(custom_month).zfill(2), None]
        frontfile_dir = 'data/external/frontfile/{}/{}'.format(year, month)
        ftp.cwd(frontfile_dir)
        frontfile_dir_list = [frontfile_dir + '/' + day for day in ftp.nlst()]
    elif custom_day is not None and custom_month is None or custom_year is None:
        raise ValueError('Please specify month and year to load a frontfile for a specific date.')
    else:
        # specific date mode
        year, month, day = [str(custom_year), str(custom_month).zfill(2), str(custom_day).zfill(2)]
        frontfile_dir = 'data/external/frontfile/{}/{}/{}'.format(year, month, day)
        frontfile_dir_list = [frontfile_dir]

    ftp.cwd(parent_dir)

    # iterating over a list of directory paths
    df = pd.DataFrame()
    for ff_dir in frontfile_dir_list:
        try:
            ftp.cwd(parent_dir)
            ftp.cwd(ff_dir)
        except ftplib.error_perm:
            logger.warning('Directory for ({}) does not exist. Terminating and writting backlog'.format(ff_dir))
            # writting backlog
            with open(os.path.join(path,'schembl_backlog.txt'), 'a') as f:
                f.write(ff_dir+'\n')
            sys.exit()

        tsv_dir_dict = get_tsv_dir(ftp)
        if not tsv_dir_dict:
            continue

        ftp.cwd(parent_dir)
        frontfile_df = get_frontfile_df(tsv_dir_dict, ftp, unique_col)
        if frontfile_df.empty:
            logger.info('Empty tsv file for: {}'.format(', '.join([value for key, value in tsv_dir_dict.items()])))
            continue

        df = pd.concat([df, frontfile_df])
        df.drop_duplicates(subset=unique_col, keep='first', inplace=True)

    ftp.quit()

    if df.empty:
        logger.info('Did not find records to write.')
        sys.exit()

    # writting fronfiles to DB
    logger.info('Loading {} data to SureChEMBL schema in DB.'.format(
        ', '.join([value for key, value in tsv_dir_dict.items()])))

    old_tbl_count = count_rows(engine, tbl_name)

    # dropping primary key and foreign key
    try:
        engine.execute("""ALTER TABLE "{0}" DROP CONSTRAINT "{0}_pkey" """.format(tbl_name))
    except Exception as e:
        logger.info('There was an issue while dropping constraints.\n{}'.format(e))

    dfloader(df, engine, tbl_name, unique_col=unique_col)
    logger.info('Finished loading {}.'.format(', '.join([value for key, value in tsv_dir_dict.items()])))

    logger.info('Adding primary key.')
    engine.execute("""ALTER TABLE "{0}" ADD PRIMARY KEY ("{1}")""".format(tbl_name, unique_col[0]))

    new_tbl_count = count_rows(engine, tbl_name)
    logger.info("""Compounds: {0}; New compounds: {1}; Final count in the DB: {2}""".format(
        len(df), new_tbl_count - old_tbl_count, new_tbl_count
        )
    )

def dfloader(
    df: pd.DataFrame,
    engine: Engine,
    tbl_name: str,
    unique_col: Optional[List[str]]=None,
    drop_duplicates: Optional[bool]=True
    ) -> None:

    '''
    Function to write pandas table SQL and drop duplicates.
    Postgres writes speed are drastically boosted due to use of COPY.
    '''

    def psql_insert_copy(table, conn, keys, data_iter):
        # borrowed form pandas docs, please see to_sql() docs
        import csv
        from io import StringIO
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    if engine.dialect.driver == 'psycopg2':
        df.to_sql(tbl_name, engine, if_exists='append', index=False, method=psql_insert_copy,
            chunksize=10**6)
    else:
        df.to_sql(tbl_name, engine, if_exists='append', index=False, method='multi',
            chunksize=10**6)

    # dropping duplicates in SQL table
    if drop_duplicates is True:
        if engine.dialect.driver in ('psycopg2', 'mysqldb'):
            sql_query = """
                DELETE FROM "{0}" T1
                    USING "{0}" T2
                WHERE T1.ctid < T2.ctid
                    AND T1."{1}"=T2."{1}"
            """.format(tbl_name, unique_col[0])
        else:
            sql_query = """
                DELETE FROM "{0}"
                WHERE rowid not in
                (SELECT MIN(rowid)
                    FROM "{0}"
                    GROUP BY "{1}")
            """.format(tbl_name, unique_col[0])
        engine.execute(sql_query)

def surechembl_mini_client(
    ftp_user: str,
    ftp_psw: str,
    conn_info: Dict[str, str],
    postgres_schema: Optional[str]=None,
    frontfile: Optional[bool] = True,
    custom_day: Optional[int]=None,
    custom_month: Optional[int]=None,
    custom_year: Optional[int]=None,
    start_year: Optional[int]=1950,
    end_year: Optional[int]=2018
    ) -> None:

    global tbl_name
    tbl_name = 'schembl_chemical_structure'
    unique_col = ['schembl_chem_id']

    path = os.path.dirname(os.path.abspath(__file__))
    global logger
    logger = AppLogger.get(
        __name__,
        os.path.join(path, '{0}.log'.format(os.path.split(__file__)[-1].strip('.py'))),
        stream_level=logging.INFO)

    # creating engine for database
    if postgres_schema is not None and conn_info['drivername'] == 'postgresql+psycopg2':
        engine = create_engine(URL(**conn_info),
            connect_args={'options':'-csearch_path={0}'.format(postgres_schema)})
    elif conn_info['database'] == 'sqlite://':
        engine = create_engine('sqlite://')
    else:
        engine = create_engine(URL(**conn_info))

    # testing connection
    try:
        connection = engine.connect()
        connection.close()
    except exc.SQLAlchemyError as e:
        raise exc.SQLAlchemyError("Failed to connect to '{0}'. Terminating.".format(engine.url.database))

    # checking if SQL table exists
    if not engine.dialect.has_table(engine, tbl_name):
        logger.warning('Destination SQL table ({0}) does not exists in DB.'.format(tbl_name))

        logger.info('Creating SQL table')
        meta = MetaData()
        schembl_tbl = Table(
           tbl_name, meta,
           Column('schembl_chem_id', Integer, primary_key=True),
           Column('smiles', Text),
           Column('std_inchi', Text),
           Column('std_inchikey', String(27))
        )
        meta.create_all(engine)

    logger.info('\nRetrieving a map for SureChEMBL to InChI.')

    if frontfile is True:
        load_frontfile(engine, unique_col, logger, ftp_user, ftp_psw, custom_day=custom_day, custom_month=custom_month, custom_year=custom_year)
    else:
        load_backfile(engine, unique_col, logger, ftp_user, ftp_psw, start_year=start_year, end_year=end_year)

def main():

    import argparse

    driver_dict = {
        'postgres':'postgresql+psycopg2',
        'oracle':'oracle+cx_oracle',
        'mysql':'mysql+mysqldb',
        'sqlite':'sqlite',
        'sqlite':'pysqlite'
    }

    parser = argparse.ArgumentParser(
        description='''SureChEMBL data client for retrieval of compound structures.''',
        epilog='Example usage in CLI:"')
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    # FTP server credentials
    required.add_argument('-fu', '--ftp_usr',
        help='User name for FTP server login.', required=True, type=str)
    required.add_argument('-fp', '--ftp_psw',
        help='Password for FTP server login.', required=True, type=str)

    # Database connection arguments
    required.add_argument('-du', '--db_usr',
        help='Database user name.', required=True,
        type=str)
    required.add_argument('-dp', '--db_psw',
        help='Database password.', required=True,
        type=str)
    required.add_argument('-dh', '--db_host',
        help='Database host.', required=True,
        type=str)
    required.add_argument('-port', '--db_port',
        help='Database port.', required=True, default='5432',
        type=str)
    required.add_argument('-dn', '--db_name',
        help='Database name.', required=True,
        type=str)
    required.add_argument('-dt', '--db_type',
        help='Database type.', required=True, choices=list(driver_dict.keys()),
        type=str)

    optional.add_argument('-schema', '--postgres_schema',
        help='Schema to write to if writting to PostgreSQL. Only works if db_type is Postgres.', required=True,
        type=str)

    # Frontfile and backfile arguments
    optional.add_argument('-ff', '--frontfile',
        help='Fronfile fetching mode. Not including the flag defaults to backfiles.', action='store_true')

    optional.add_argument('-cd', '--custom_day',
        help='Fronfile fetching mode. Specify day.', default=None, type=int)
    optional.add_argument('-cm', '--custom_month',
        help='Fronfile fetching mode. Specify month.', default=None, type=int)
    optional.add_argument('-cy', '--custom_year',
        help='Fronfile fetching mode. Specify year.', default=None, type=int)
    optional.add_argument('-sy', '--start_year',
        help='Fronfile fetching mode. Specify start year for year range.', default=1950, type=int)
    optional.add_argument('-ey', '--end_year',
        help='Fronfile fetching mode. Specify end year for year range.', default=2018, type=int)
    args = parser.parse_args()

    # database type to drivername
    conn_info = {
        'drivername' : driver_dict[args.db_type.lower()],
        'username' : args.db_usr,
        'password' : args.db_psw,
        'host' : args.db_host,
        'port' : args.db_port,
        'database' : args.db_name
    }

    surechembl_mini_client(args.ftp_usr, args.ftp_psw, conn_info, args.postgres_schema, args.frontfile,
        args.custom_day, args.custom_month, args.custom_year, args.start_year, args.end_year)

if __name__ == "__main__":

    main()
