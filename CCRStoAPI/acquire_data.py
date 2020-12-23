import logging
import snowflake.connector
from .config import DB_PASSWORD, DB_SCHEMA, ACQUIRE_DATA_QUERY


def acquire_data():
    logging.info('acquire_data'.upper())
    df = get_data_from_snowflake()
    data = []
    for index, row in df.iterrows():
        data.append(row)
    return data


def get_data_from_snowflake():
    logging.info('get_data_from_snowflake'.upper())
    try:
        con = get_connection()
        results_iterable = con.cursor().execute(ACQUIRE_DATA_QUERY)
        logging.info("Total Records: " + str(results_iterable.rowcount))
        df = results_iterable.fetch_pandas_all()
        logging.info(df)
        return df
    except Exception as e:
        logging.error("Exception in get_data_from_snowflake: " + str(e))
        return e


def get_connection():
    logging.info('get_connection'.upper())
    return snowflake.connector.connect(
        user='CANOTIFY_USER',
        password=DB_PASSWORD,
        account='your_snowflake_account',
        database='CA_NOTIFY',
        schema=DB_SCHEMA,
        warehouse='VWH_CA_NOTIFY',
        role='CA_NOTIFY_ROLE'
    )
