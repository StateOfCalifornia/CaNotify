import logging
import time
import random
import string
import base64
from .config import EPOCH_COUNT, BATCH_SIZE, SLEEP_TIME
from .acquire_data import acquire_data
from .pre_check import pre_check_and_annotate
from .write_back import write_back
from .verification_server import call_verification_server, evaluate_response


def orchestrate_epochs():
    logging.info("orchestrate_epochs".upper())
    t0 = time.time()
    for x in range(0, EPOCH_COUNT):
        execute_single_epoch(x)
    t1 = time.time()
    total_time = t1 - t0
    logging.info("TIME: " + str(total_time))


def execute_single_epoch(epoch_num):
    logging.info(f"execute_single_epoch: ${epoch_num}".upper())
    data = acquire_data()
    t0 = time.time()
    if len(data):
        orchestrate_batches(data)
    else:
        logging.info("Data length 0. Skipping processing")
    t1 = time.time()
    sleeptime = SLEEP_TIME - (t1 - t0)
    logging.info("Sleeptime: " + str(sleeptime))
    if sleeptime > 0:
        time.sleep(sleeptime)


def orchestrate_batches(data):
    logging.info("orchestrate_batches")
    batches = create_batches(data)
    for index, batch in enumerate(batches):
        logging.info("Now processing batch: " + str(index))
        execute_single_batch(batch)


def create_batches(data):
    logging.info("create_batches")
    for i in range(0, len(data), BATCH_SIZE):
        yield data[i:i + BATCH_SIZE]


def execute_single_batch(batch):
    logging.info("process_batch: ".upper())
    valid_recipients, invalid_recipients = generate_recipient_lists(batch)
    data_object = {"codes": valid_recipients}
    response = call_verification_server(data_object)
    valid_recipients_evaluated = evaluate_response(response, valid_recipients)
    write_back(valid_recipients_evaluated, invalid_recipients)


def generate_recipient_lists(batch):
    logging.info("generate_recipient_lists: ".upper())
    objects_list = generate_request_objects_list(batch)
    annotated_list = pre_check_and_annotate(objects_list)
    valid_recipients = list(filter(
        lambda x: x['pre_check'] is True, annotated_list))
    invalid_recipients = list(filter(
        lambda x: x['pre_check'] is False, annotated_list))
    # Remove annotations
    for item in valid_recipients:
        del item['pre_check']
    for item in invalid_recipients:
        del item['pre_check']
    return valid_recipients, invalid_recipients


def generate_request_objects_list(batch):
    logging.info("generate_list_of_request_objects: ".upper())
    objects_list = []
    for row in batch:
        request_obj = generate_request_object(row)
        objects_list.append(request_obj)
    return objects_list


def generate_request_object(row):
    logging.info("create_request_object: ".upper() + str(row))
    return {"testDate": str(row.DATE),
            "testType": str(row.TESTTYPE),
            "tzOffset": -420,
            "phone": str(row.PHONE),
            "padding": get_random_base64_string(),
            "pre_check": False}


def get_random_base64_string():
    logging.info("get_random_base64_string".upper())
    length = random.randint(5, 20)
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return base64.b64encode(result_str.encode('utf-8')).decode('ascii')
