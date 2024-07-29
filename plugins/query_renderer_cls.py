from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
import configparser

class CustomSQLOperator(SQLExecuteQueryOperator):
   
    def __init__(self, sql_file, conn_id, parameters=None, *args, **kwargs):
        super().__init__(sql='', conn_id=conn_id, parameters=parameters, *args, **kwargs)
        self.sql_file = sql_file
        self.parameters = parameters
        self.add_attr_c_path = parameters["add_attr_c_path"]
        self.etl_cfg_path = parameters["etl_cfg_path"]
        self.hercules_home = parameters["hercules_home"]
        self.perseus_home = parameters["perseus_home"]

    def log_fmtd_sql(self, file_dir,file_name,etl_full_sqls):
        with open(file_dir+"/log/{}".format(file_name), "w") as f:
            f.write(etl_full_sqls)
            print(f"{file_name} log file written")
            f.close()

    def render_sql(self, sql_query,tablename, add_attr_c_path,etl_cfg_path):
        etl_full_sql_ls =[]
        etl_full_sql_ls.append(sql_query)

        try:
            with open(add_attr_c_path+"/"+tablename+"_ADD_ATTR_C.sql",'r') as addattr_file:
                sql_query_add_attr = addattr_file.read()
                etl_full_sql_ls.append(sql_query_add_attr)
        except Exception as e:
            print(f'no {tablename}_add_attr_c found',e)

        try:
            with open(add_attr_c_path+"/"+tablename+"_C.sql",'r') as _c_file:
                sql_query_C = _c_file.read()
                etl_full_sql_ls.append(sql_query_C)
        except Exception as e:
            print(f'no {tablename}_c found',e)
        
        etl_full_sqls = '\n'.join(etl_full_sql_ls)

        # Open config file (etl.cfg)
        config = configparser.ConfigParser(dict_type = dict)
        config.read(etl_cfg_path)
        print(f"Read etl.cfg from {etl_cfg_path}")
        
        if not config.has_section(tablename.lower()):
            print("adding section: " + tablename.lower())
            config.add_section(tablename.lower())
        
        try:
            config.set(tablename.lower(), 'hercules_home', self.hercules_home) #If the given section exists, set the given option to the specified value; otherwise raise NoSectionError. option and value must be strings;
            config.set(tablename.lower(), 'perseus_home', self.perseus_home)
            params = config[(tablename.lower())]                             # config[table_name.lower()]
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

        rendered_sql = etl_full_sqls
        self.log_fmtd_sql(file_dir= 'sql', file_name = tablename + ".sql", etl_full_sqls = rendered_sql)
        return rendered_sql
    
    def run_rendered_sql(self,redendered_sql,context):
        self.log.info("Executing SQL query")
        super().execute(context)
        self.log.info("SQL query executed successfully")
        
    def execute(self, context)-> None:
        # Read the SQL file
        self.log.info(f"Reading SQL file: {self.sql_file}")
        tablename = self.sql_file.split('/')[1].split('.')[0]+'.'+self.sql_file.split('.')[1]
        
        with open(self.sql_file, 'r') as file:
            sql_query = file.read()
        
        # Render the SQL query
        self.log.info("Rendering SQL query with parameters")
        rendered_query = self.render_sql(sql_query,tablename, self.add_attr_c_path, self.etl_cfg_path)
        # Set the SQL query to be executed
        self.sql = rendered_query
        # Execute the SQL query using the parent class's execute method
        self.run_rendered_sql(rendered_query,context)

'''# Example usage in a DAG
# Example task using CustomSQLOperator
task = CustomSQLOperator(
    task_id='run_sql',
    sql_file='/path/to/your/sql/file.sql',
    conn_id='your_connection_id',
    parameters={'param1': 'value1', 'param2': 'value2'},
    dag=dag,
)
'''