import pandas as pd
import datetime
import pymysql
import multiprocessing

# Define your database connection parameters
host = host
port = port
user = user
password = password
database = db_name




def query_string_dynamic(start_date, end_date):
    query_data = f"""
       select * from 
    and date >= '{start_date}'
    and date < '{end_date}';
    """
    return query_data

def fetch_data_multiprocess(start_date, end_date):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=database)
    query_data = query_string_dynamic(start_date, end_date)
    temp_data = pd.read_sql(query_data, conn)
    conn.close()
    return temp_data


def fetch_and_concat_data(args):
    start_date, end_date = args
    try:
        parameter = {'start_date1': start_date, 'end_date1': end_date}
        print(parameter)
        data = fetch_data_multiprocess(start_date, end_date)
        print(data.shape)
        return data
    except:
        fetch_and_concat_data(args)


if __name__ == "__main__":
    cpu_cores = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(cpu_cores)

    end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=0)
    start_date = end_date - datetime.timedelta(days=23)

    date_ranges = []
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + datetime.timedelta(hours=2)
        print(f"Start Date: {current_date}, End Date: {next_date}")
        date_ranges.append((current_date.strftime('%Y-%m-%d %H:%M:%S'), next_date.strftime('%Y-%m-%d %H:%M:%S')))
        current_date = next_date

    results = pool.starmap(fetch_data_multiprocess, date_ranges)
    pool.close()
    pool.join()

    df = pd.concat(results, ignore_index=True)

    today_str = datetime.datetime.now().strftime('%Y-%m-%d')

    master_output_file = f'data.csv'
    df.to_csv(master_output_file, index=False)
