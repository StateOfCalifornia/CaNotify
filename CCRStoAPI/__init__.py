import azure.functions as func
import logging
from .process_data import orchestrate_epochs


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("main".upper())
    orchestrate_epochs()
    return func.HttpResponse('Completed')
