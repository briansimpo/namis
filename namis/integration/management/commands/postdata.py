
import os
from django.core.management.base import BaseCommand, CommandError
from namis.integration.services import Processor

class Command(BaseCommand):
    help = 'Process the file specified by the filepath'

    def add_arguments(self, parser):
        parser.add_argument('filepath', type=str, help='The path to the file to be processed')

    def handle(self, *args, **kwargs): 
        filepath = kwargs['filepath']

        if not os.path.isfile(filepath):
            raise CommandError(f'File "{filepath}" does not exist.')
        processor = Processor()
        processor.read(filepath)

       