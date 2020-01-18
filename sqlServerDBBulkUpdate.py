import pyodbc
import pandas as pd

db_username = 'edkent'
db_pass = 'TrlDev1*'
db_name = 'TRL_DBTest'
db_schema = 'dbo'
db_url = '65.17.225.237'
# db_url = '65.17.225.237,5432'
db_table_name = 'products'
db_col_to_update = 'cstock'
db_col_key = 'ccode'
db_full_table_name = '{}.{}.{}'.format(db_name, db_schema, db_table_name)
driver_name = 'ODBC Driver 13 for SQL Server'
# driver_name = 'SQL Server'
cursor = None
cnxn = None

def init_db():
    # print("Available drivers to connect are - ", pyodbc.drivers())
    try:
        global cnxn
        global cursor

        connection_string = 'DRIVER={};SERVER={};DATABASE={};UID={};PWD={}'.format(driver_name, db_url, db_name, db_username, db_pass)
        
        print('Conneting to DB - ', connection_string)
        cnxn = pyodbc.connect(connection_string, autocommit=True)
        cursor = cnxn.cursor()
        print('Current connection supports ',cnxn.getinfo(pyodbc.SQL_MAX_CONCURRENT_ACTIVITIES), 'cursors')
        print('Selecting version from DB...', end='\n')
        cursor.execute("SELECT @@version;")
        row = cursor.fetchone()

        while row:
            print(row[0])
            break

        print('Conection successfull.')
        # now initiating the process of adta update.
        start_update_process()

        print('Closing connection')
        cursor.close()
        cnxn.commit()
        cnxn.close()
        print('Connection closed.')
    except Exception as e:
        print(e)
    



target_sheet="Master sheet"
excel_col1="Product Code (as per barcode , no *)"
excel_col2="Count Qty"
excel_file_path = 'C:/Users/workstation/Desktop/Master - new.xlsx'



def start_update_process():
    excel_read_and_parse_status = read_data_from_excel()

    if excel_read_and_parse_status is True:
        temp_table_create_status = create_temp_table_from_db_data()

        if temp_table_create_status is True:
            # prepare_temp_table_update_queries_status = prepare_temp_table_update_queries()
            insert_new_data_status = insert_new_data_into_temp_table()

            if insert_new_data_status is True:
                # Finally perform the innerjoin on db table and temp table.
                db_update_status = update_db_with_new_data()

                if db_update_status is True:
                    # dropping temp table from db.
                    if drop_temp_table() is True:
                        print('\nDB is updated with new data({}, {}) from file {}'.format(excel_col1, excel_col2, excel_file_path))




""" TODO
    1). Make a DB connection
    2). if successfull then fetch the data from excel file
    3). transform data into list {data_in_list_of_dict} of objects with 2 keys, {excel_col1, excel_col2}.
    4). create the temp table with columns data from db table {db_table_name}.
    5). create the list of update temp table command with data from step 3. (data_in_list_of_dict)
    Now that temp table is filled with copy of data from original data from db table {db_table_name}
    and update commands are also ready to update the new data into temp table
    6). Execute the update temp table command in batch size of 100.
    Now that temp table is having new data from excel sheet of column {excel_col1, excel_col2}
    7). Join the original db table and temp table using inner join on id -> {excel_col1 == db_col_key}
"""
excel_data = None
data_in_list_of_dict = None



# excel_file_path="/home/no-one/Downloads/lancing/Master - new.xlsx"


def read_data_from_excel():
    # excel_file_path = input('Enter excel path: ')

    try:
        global data_in_list_of_dict
        print('Reading excel file ', )
        excel_data = pd.read_excel(excel_file_path, sheet_name=target_sheet, usecols=[excel_col1, excel_col2])
        data_in_list_of_dict = excel_data.to_dict(orient='record')
        print('Excel data transformed into list of objects of length ', len(data_in_list_of_dict), 'of type ', data_in_list_of_dict[0])

        return True

    except Exception as e:
        print('Error in reading file ', excel_file_path)
        return False


# temptable_name = '#{}_temp_{}_table'.format(db_name, db_table_name)
temptable_name = '{}.{}.{}_temp_table'.format(db_name, db_schema, db_table_name)

def create_temp_table_from_db_data():
    try:
        temp_table_create_cmd = '''
        if OBJECT_ID('{}', 'U') is null
        create table {}({} nvarchar(50), {} float)

        '''.format(temptable_name, temptable_name, db_col_key, db_col_to_update)
        # temp_table_create_cmd = 'select {}, {} into {} from {}'.format(db_col_key, db_col_to_update, temptable_name, db_full_table_name)
        # temp_table_create_cmd = 'create table {}({} nvarchar(50), {} real)'.format(temptable_name, db_col_key, db_col_to_update)
        print('Creating Temp table using query - ', temp_table_create_cmd)
        with cnxn:
            res = cursor.execute(temp_table_create_cmd)
            res.commit()
        print('Temp table {} created.'.format(temptable_name))
        return True
    except Exception as e:
        print('Error in temp table creation.', e)
        return False


bulk_insert_queries = []
def prepare_bulk_insert_queries():
    global bulk_insert_queries
    for obj in data_in_list_of_dict:
        v2 = obj[excel_col2]
        if obj[excel_col2] != obj[excel_col2]:
            v2 = 0
        bulk_insert_queries.append((obj[excel_col1], v2))

    print('Bulk insert queries prepared of type: ', bulk_insert_queries[0:3])


def insert_new_data_into_temp_table():
    try:
        #convert data_in_list_of_dict from [{},{}] to list[[],[]]
        prepare_bulk_insert_queries()
        bulk_insert_cmd = '''
        insert into {}({}, {}) values(?, ?)
        '''.format(temptable_name, db_col_key, db_col_to_update)
        print('Executing bulk insert command...', bulk_insert_cmd)

        with cnxn:
            cursor.fast_executemany = True
            cursor.executemany(bulk_insert_cmd, bulk_insert_queries)
            cursor.commit()
            print('Bulk Insert successfull.')
        return True
    except Exception as e:
        print('Error in bulk insert: ', e)
        return False

# bulk_update_query_list = []
bulk_update_temp_table_query_list = []


def get_temp_table_update_cmd(_id, value):
    update_temp_table_cmd = 'update {} set {}={} where {}="{}"'.format(temptable_name, db_col_to_update, value, db_col_key, _id)
    return update_temp_table_cmd

def prepare_temp_table_update_queries():
    global bulk_update_temp_table_query_list
    for obj in data_in_list_of_dict:
        bulk_update_temp_table_query_list.append(get_temp_table_update_cmd(obj[excel_col1], obj[excel_col2]))

    print('Temp table update queries created of type: ', bulk_update_temp_table_query_list[0])




def update_db_with_new_data():
    cmd = 'update A set A.{}=T.{} from {} A INNER JOIN {} T on A.{}=T.{}'.format(db_col_to_update, db_col_to_update, db_full_table_name, temptable_name ,db_col_key, db_col_key)
    try:
        print('Running INNER JOIN on DB table {} with temp table {}'.format(db_full_table_name, temptable_name))
        print('using query: ', cmd)
        input('Are you sure you want to continue?')
        with cnxn:
            cursor.execute(cmd)
            cursor.commit()
        print('INNER JOIN successful.')
        return True
    except Exception as e:
        print('Error on INNER JOIN ', e)
        return False


def drop_temp_table():
    query = '''
    if OBJECT_ID('{}', 'U') is not null
    drop table {};
    '''.format(temptable_name, temptable_name)

    try:
        print('Dropping temp table {}'.format(temptable_name))
        with cnxn:
            cursor.execute(query)
            cursor.commit()
        print('Table dropped successful.')
        return True
    except Exception as e:
        print('Error in clearing up temp storage from DB. ', e)
        return False






print('Running ', __name__)
if __name__ == '__main__':
    init_db()




