import logging
from .config import INSERT_WRITEBACK_QUERY, VAL
from .acquire_data import get_connection


def write_back(valid_recipients_evaluated, invalid_recipients):
    logging.info("write_back - V: ".upper() + str(valid_recipients_evaluated))
    logging.info("write_back - I: ".upper() + str(invalid_recipients))
    con = get_connection()

    query_string = INSERT_WRITEBACK_QUERY
    query_string += ','.join( [VAL.format(item["phone"],0) for item in invalid_recipients ] + [VAL.format(item["phone"],item["result"]) for item in valid_recipients_evaluated ])
    con.cursor().execute(query_string)

    return
