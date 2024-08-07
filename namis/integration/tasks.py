from celery import shared_task
from .services import Processor

@shared_task
def  post_file(filepath):
    try:
        processor = Processor()
        processor.upload(filepath)
    except:
        pass
    