from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

def get_hook(conn_id):
    hookconn  = RedshiftSQLHook.get_hook(conn_id)
    return hookconn

def insert_counts(conn_id, schema, module):
    rshook = get_hook(conn_id)
    schema_name = schema
    module_name = module

    sql_stmt = "INSERT INTO "+schema_name+".DW_Counts(module, datestamp, tbl_name, tbl_count) select '" \
        +module_name+"' as module ,convert_timezone('America/New_York',sysdate) as datestamp, tab_name, nvl(tbl_rows,0) from " \
        +schema_name+".dw_table_list a left join  SVV_TABLE_INFO b ON b.\"schema\" = a.schema_name and b.\"table\" = a.tab_name where a.module='" \
        +module_name+"' order by 1;"
    rshook.run(sql= sql_stmt,autocommit = True)

def get_counts(conn_id, schema, module):
    rshook = get_hook(conn_id)
    schema_name = schema
    module_name = module
    sql_stmt ="select tbl_name, to_char(tbl_count,'999,999,999') as tbl_count from " \
            +schema_name+".dw_counts where datestamp=(select max(datestamp) from " \
            +schema_name+".dw_counts where module='"+module_name+"' ) and module ='"    \
            +module_name+"' order by 1,2 ;"
    rows = rshook.get_records(sql= sql_stmt)
    return rows