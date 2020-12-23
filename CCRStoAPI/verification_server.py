import logging
import json
import requests
from .config import APHL_ADMIN_URL, APHL_ADMIN_API_KEY


def call_verification_server(request_obj):
    logging.info("call_verification_server: ".upper() + str(request_obj))
    try:
        response = requests.post(APHL_ADMIN_URL, data=json.dumps(request_obj),
                headers={
                    "content-type": "application/json",
                    "accept": "application/json",
                    "x-api-key": APHL_ADMIN_API_KEY})
        logging.info("Google response = " + str(response))
        return response.json()
    except Exception as e:
        logging.info("Google Exception = " + str(e))
        return e


def evaluate_response(response, valid_recipients):
    logging.info("evaluate_response: ".upper())
    if(response.get("error", "")):
        logging.info("Error in Google response".upper())
        for item in valid_recipients:
            singletonresponse = call_verification_server({"codes": [item]})
            if(singletonresponse.get("error")):
                item["result"] = 0
            else:
                item["result"] = 1
    else:
        logging.info("No errors in Google response".upper())
        for item in valid_recipients:
            item["result"] = 1
    return valid_recipients
