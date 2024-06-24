import sys
import re
import string
import collections
import os
import glob
import csv
import psycopg2
import json
import configparser
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models import Variable

class sql_executor(BaseOperator):
    def __init__(self, filedir:str, filename:str, schemaname:str, tablename:str, modulename:str, env:str, conn_id:str,**kwargs)-> None:
        super().__init__(**kwargs)
        self.filedir = filedir 
        self.filename = filename
        self.schemaname = schemaname
        self.tablename = tablename
        self.modulename = modulename
        self.env = env
        self.conn_id= conn_id

        if env == 'prod':
            self.hercules_home = 'hercules'
            self.perseus_home = 'perseus'
        elif env == 'qat':
            self.hercules_home = 'hercules_qat'
            self.perseus_home = 'perseus_qat'

def read_sql_file(self, filename):
    # Open the sql file for parsing, read only
    etl_sql_file = open(filename,'r')
    etl_full_sqls = etl_sql_file.read()
    
    # Open config file (etl.cfg)
    config = configparser.ConfigParser(dict_type = dict)
    config.read(self.filedir+"/ADD_ATTR_C/etl.cfg")
    print(" reading from "+self.filedir+"/ADD_ATTR_C/etl.cfg")

    if not config.has_section(self.tablename.lower()):
        print("adding section: " + self.tablename.lower())
        config.add_section(self.tablename.lower())

    try:
        config.set(self.tablename.lower(), 'hercules_home', self.hercules_home) #If the given section exists, set the given option to the specified value; otherwise raise NoSectionError. option and value must be strings;
        config.set(self.tablename.lower(), 'perseus_home', self.perseus_home)
        params = config[(self.tablename.lower())]                             # config[table_name.lower()]
        print(dict(params))
        # insert custom parameters
        etl_full_sqls = etl_full_sqls.format( **params )
        
        try:
            etl_full_sqls = etl_full_sqls.format( **params )
        
        except Exception as e:
            print('no params',e)
        
        else:
            etl_full_sqls = etl_full_sqls    
                
    except Exception as e:
        print('no params',e)
    return etl_full_sqls


def log_fmtd_sql(self, file_dir,file_name,etl_full_sqls):
    with open(file_dir+"log\\{}".format(file_name), "w") as f:
        f.write(etl_full_sqls)
        f.close()

def exec_etl_sql(self,etl_full_sqls,conn):
    cur = conn.cursor()
    etl_sqls = etl_full_sqls.split(';')
    for etl_sql in etl_sqls:
        print(len(etl_sql.strip()))
        if len(etl_sql.strip()) == 0:    
            print("zero condition check")
            print(len(etl_sql.strip()))
            continue
        else:
            print(etl_sql)    
            if etl_sql != "":
                cur.execute(etl_sql)

        
        etl_sqls = etl_full_sqls.split(';')        
        cur = conn.cursor()
        try:
            etl_sql()
        except Exception as e1:
            print("First attempt failed. Rolling back...Retrying")
            conn.rollback()
            print(e1)
            try:
                etl_sql()
            except Exception as e2:
                print("Second attempt failed. Rolling back...")
                conn.rollback()
                print(e2)
                exit(1)
    conn.commit()

def execute(self, context)-> None:
        print(context)
        print(f'env = {self.env}')
        self.etl_sql(self.filedir, self.filename, self.schemaname, self.tablename,self.conn)