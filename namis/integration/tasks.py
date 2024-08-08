from celery import shared_task
from .services import Processor

@shared_task(soft_time_limit=72000, time_limit=324000)  # 30days, 90days
def  post_file(filepath):
    try:
        processor = Processor()
        processor.upload(filepath)
    except:
        pass
    