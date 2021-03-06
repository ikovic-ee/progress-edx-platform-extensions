"""
One-time data migration script -- shoulen't need to run it again
"""
import logging
from optparse import make_option

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models import Q, Min

from progress.models import StudentProgress, CourseModuleCompletion
from student.models import CourseEnrollment
from xmodule.modulestore.django import modulestore

log = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Creates (or updates) progress entries for the specified course(s) and/or user(s)
    """
    help = "Command to creaete or update progress entries"
    option_list = BaseCommand.option_list + (
        make_option(
            "-c",
            "--course_ids",
            dest="course_ids",
            help="List of courses for which to generate progress",
            metavar="first/course/id,second/course/id"
        ),
        make_option(
            "-u",
            "--user_ids",
            dest="user_ids",
            help="List of users for which to generate progress",
            metavar="1234,2468,3579"
        ),
    )

    def handle(self, *args, **options):
        course_ids = options.get('course_ids')
        user_ids = options.get('user_ids')

        detached_categories = getattr(settings, 'PROGRESS_DETACHED_CATEGORIES', [])
        cat_list = [Q(content_id__contains=item.strip()) for item in detached_categories]
        cat_list = reduce(lambda a, b: a | b, cat_list)

        # Get the list of courses from the system
        courses = modulestore().get_courses()

        # If one or more courses were specified by the caller, just use those ones...
        if course_ids is not None:
            filtered_courses = []
            for course in courses:
                if unicode(course.id) in course_ids.split(','):
                    filtered_courses.append(course)
            courses = filtered_courses

        for course in courses:
            users = CourseEnrollment.objects.users_enrolled_in(course.id)
            # If one or more users were specified by the caller, just use those ones...
            if user_ids is not None:
                filtered_users = []
                for user in users:
                    if str(user.id) in user_ids.split(','):
                        filtered_users.append(user)
                users = filtered_users

            # For each user...
            for user in users:

                status = 'skipped'
                num_completions = CourseModuleCompletion.objects.filter(course_id=course.id, user_id=user.id)\
                    .exclude(cat_list).count()

                if num_completions:

                    start_date = CourseModuleCompletion.objects.filter(course_id=course.id, user_id=user.id)\
                        .exclude(cat_list).aggregate(Min('created'))['created__min']

                    try:
                        existing_record = StudentProgress.objects.get(user=user, course_id=course.id)

                        if existing_record.completions != num_completions:
                            existing_record.completions = num_completions
                            status = 'updated'

                        if existing_record.created != start_date:
                            existing_record.created = start_date
                            status = 'updated'

                        if status == 'updated':
                            existing_record.save()

                    except StudentProgress.DoesNotExist:
                        StudentProgress.objects.create(
                            user=user,
                            course_id=course.id,
                            completions=num_completions,
                            created=start_date
                        )
                        status = 'created'

                log_msg = 'Progress entry {} -- Course: {}, User: {}  (completions: {})'.format(
                    status,
                    course.id,
                    user.id,
                    num_completions
                )

                print log_msg
                log.info(log_msg)
