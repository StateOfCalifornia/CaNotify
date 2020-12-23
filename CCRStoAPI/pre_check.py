import logging
import threading
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
from .config import ACCOUNT_SID, AUTH_TOKEN


twilio_client = Client(ACCOUNT_SID, AUTH_TOKEN)


def pre_check_and_annotate(objects_list):
    logging.info("execute_pre_check: ".upper())
    annotated_list = []
    threads = []
    for request_obj in objects_list:
        logging.info("Creating Thread For: ".upper() + str(request_obj))
        annotated_list.append(request_obj)
        x = threading.Thread(target=is_phone_valid, args=(request_obj,))
        threads.append(x)
    for thread in threads:
        logging.info("Starting Thread: ".upper() + str(thread))
        thread.start()
    for thread in threads:
        logging.info("Joining Thread: ".upper() + str(thread))
        thread.join()
    return annotated_list


def is_phone_valid(request_obj):
    logging.info("is_phone_valid: ".upper() + str(request_obj))
    if twilio_lookup_is_valid_mobile(request_obj["phone"]):
        request_obj["pre_check"] = True
    else:
        request_obj["pre_check"] = False


def twilio_lookup_is_valid_mobile(phone):
    logging.info("validate_phone_number: ".upper() + str(phone))
    try:
        phone_number = twilio_client.lookups.phone_numbers(
            str(phone)).fetch(type=['carrier'])
        if (phone_number.carrier['type']
                and phone_number.carrier['type'] != "mobile") \
                or len(phone) < 10 or len(phone) > 11:
            return False
        else:
            return True
    except TwilioRestException as e:
        logging.info("Exception: " + str(phone) + " " + str(e))
        return False
