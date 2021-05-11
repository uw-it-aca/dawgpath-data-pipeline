from django.core.management.base import BaseCommand
from prereq_data_pipeline.dao.edw import get_prereqs, \
    get_curric_info, get_course_titles


class Command(BaseCommand):
    def handle(self, *args, **options):
        print(get_course_titles())
        print(get_curric_info())
