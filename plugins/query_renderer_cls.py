from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.decorators import apply_defaults
import os

class CustomSQLOperator(SQLExecuteQueryOperator):
    template_fields = ('sql_file', 'parameters')
    template_ext = ('.sql',)
    
    @apply_defaults
    def __init__(self, sql_file, conn_id, parameters=None, *args, **kwargs):
        super(CustomSQLOperator, self).__init__(sql='', conn_id=conn_id, parameters=parameters, *args, **kwargs)
        self.sql_file = sql_file
        self.parameters = parameters or {}
    
    def render_sql(self, sqlpath, parampath):
        rendered_sql = sqlpath
        return rendered_sql
    
    def execute(self, context):
        # Read the SQL file
        self.log.info(f"Reading SQL file: {self.sql_file}")
        with open(self.sql_file, 'r') as file:
            sql_query = file.read()
        
        # Render the SQL query with Jinja
        self.log.info("Rendering SQL query with parameters")
        rendered_query = self.render_sql(sql_query, self.parameters)
        
        # Set the SQL query to be executed
        self.sql = rendered_query
        
        # Execute the SQL query using the parent class's execute method
        self.log.info("Executing SQL query")
        super(CustomSQLOperator, self).execute(context)
        self.log.info("SQL query executed successfully")

'''# Example usage in a DAG
from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'example_custom_sql_operator',
    default_args=default_args,
    description='A simple example DAG using CustomSQLOperator',
    schedule_interval=None,
)

# Example task using CustomSQLOperator
task = CustomSQLOperator(
    task_id='run_sql',
    sql_file='/path/to/your/sql/file.sql',
    conn_id='your_connection_id',
    parameters={'param1': 'value1', 'param2': 'value2'},
    dag=dag,
)
'''