#!/usr/bin/python3
"""
Run validation checks on Enterprise data.
"""

from __future__ import absolute_import, unicode_literals

import argparse
import csv
import datetime
import itertools
import logging
import os
import sys
from collections import defaultdict

import mysql.connector
import vertica_python


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

ENTERPRISE_CUTOFF_DATE = datetime.datetime(2017, 5, 1)

VERTICA_QUERY = '''
    SELECT
        enterprise_user_id,
        lms_user_id,
        enterprise_sso_uid,
        enterprise_id,
        enterprise_name,
        enrollment_created_timestamp,
        consent_granted,
        course_id,
        user_account_creation_date,
        user_email,
        user_username,
        user_age,
        user_level_of_education,
        user_gender,
        user_country_code,
        country_name,
        has_passed,
        last_activity_date,
        user_current_enrollment_mode
    FROM
        business_intelligence.enterprise_enrollment
    WHERE
        {where}
'''


def fetch_lms_data(query):
    data = []
    connection_info = {
        'host': os.environ.get('MYSQL_HOST'),
        'user': os.environ.get('MYSQL_USERNAME'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'database': os.environ.get('MYSQL_DATABASE'),
    }
    try:
        connection = mysql.connector.connect(**connection_info)
        cur = connection.cursor(dictionary=True)
        cur.execute(query)
        for row in cur:
            data.append(row)
    finally:
        connection.close()
    return data


def fetch_vertica_data(query):
    connection_info = {
        'host': os.environ.get('VERTICA_HOST'),
        'user': os.environ.get('VERTICA_USERNAME'),
        'password': os.environ.get('VERTICA_PASSWORD'),
        'database': os.environ.get('VERTICA_DATABASE'),
    }
    with vertica_python.connect(**connection_info) as connection:
        cur = connection.cursor('dict')
        cur.execute(query)
        return cur.fetchall()


def log_record(record):
    logger.info('********************')
    for key, value in record.items():
        logger.info('%s: %s', key, value)
    logger.info('********************')


def export_missing_enterprise_course_enrollments(enterprise_learners):
    now = datetime.datetime.now()
    csv_filename = '../output/missing_enterprise_course_enrollments_{}.csv'.format(now.strftime('%Y%m%d%H%M'))
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            'LMS User ID',
            'LMS Username',
            'Enterprise Customer Name',
            'EnterpriseCustomerUser Created',
            'Course ID',
            'Enrollment Created',
            'Enrollment Active',
            'Enrollment Mode'
        ])
        for enterprise_learner in enterprise_learners:
            enterprise_learner.export_enrollments(writer)


class EnterpriseLearner(object):

    def __init__(self, lms_user_id, lms_user_username, enterprise_name,
                 ecu_created, course_enrollments=[]):
        self.lms_user_id = lms_user_id
        self.lms_user_username = lms_user_username
        self.enterprise_name = enterprise_name
        self.ecu_created = ecu_created
        self.course_enrollments = course_enrollments

    @property
    def learner_info(self):
        return [
            self.lms_user_id,
            self.lms_user_username,
            self.enterprise_name,
            self.ecu_created.strftime('%Y-%m-%d %H:%M:%S'),
        ]

    def enrollment_info(self, exclude_non_enterprise=True):
        enrollments = []
        for enrollment in self.course_enrollments:
            enrollment_created = enrollment['created']
            if not exclude_non_enterprise or enrollment_created >= self.ecu_created:
                enrollments.append([
                    enrollment['course_id'],
                    enrollment['created'].strftime('%Y-%m-%d %H:%M:%S'),
                    str(enrollment['is_active']),
                    enrollment['mode']
                ])
        return enrollments

    def export_enrollments(self, writer):
        for enrollment in self.enrollment_info():
            writer.writerow(self.learner_info + enrollment)

    def log(self):
        logger.info(self.lms_user_id)
        logger.info(self.lms_user_username)
        logger.info(self.enterprise_name)
        logger.info(self.ecu_created)
        for enrollment in self.course_enrollments:
            logger.info(
                '    %s %s %s %s',
                enrollment['course_id'],
                enrollment['created'],
                enrollment['is_active'],
                enrollment['mode']
            )


def validate_enterprise_data(enterprise_customer):
    query_filter = 'lms_user_id IS NOT NULL'
    if enterprise_customer:
        query_filter = " AND enterprise_id = '{}'".format(enterprise_customer)

    # Enterprise learners without enterprise course enrollment
    vertica_data = fetch_vertica_data(
        VERTICA_QUERY.format(where=(query_filter + ' AND course_id IS NULL'))
    )

    user_enterprise_enrollments = defaultdict(list)
    for record in vertica_data:
        lms_user_id = str(record['lms_user_id'])
        user_enterprise_enrollments[lms_user_id].append(record)
    logger.info(
        '%s Enterprise learners without enterprise course enrollment',
        len(user_enterprise_enrollments.keys())
    )

    # Which ones actually do have enterprise_enterprisecourseenrollment records?
    query = '''
        SELECT
            ece.created AS created,
            ece.modified AS modified,
            ece.course_id AS course_id,
            u.id AS lms_user_id
        FROM
            auth_user u,
            enterprise_enterprisecustomeruser ecu,
            enterprise_enterprisecourseenrollment ece
        WHERE
            ece.enterprise_customer_user_id = ecu.id AND
            ecu.user_id = u.id AND
            u.id in ({user_ids})
    '''.format(
        user_ids=','.join(user_enterprise_enrollments.keys())
    )
    enterprise_course_enrollments = fetch_lms_data(query)

    for record in enterprise_course_enrollments:
        log_record(record)
        lms_user_id = str(record['lms_user_id'])
        vertica_records = user_enterprise_enrollments.pop(lms_user_id)
        for vertica_record in vertica_records:
            log_record(vertica_record)
    logger.info('%s actually do have enterprise_enterprisecourseenrollment records', len(enterprise_course_enrollments))

    # Which ones do not have enterprise_enterprisecourseenrollment records?
    vertica_data = list(itertools.chain.from_iterable(user_enterprise_enrollments.values()))
    logger.info(
        '%s Enterprise learners without enterprise course enrollment',
        len(user_enterprise_enrollments.keys())
    )

    # Which ones have student_courseenrollment records?
    query = '''
        SELECT
            user_id AS lms_user_id,
            course_id,
            created,
            is_active,
            mode
        FROM
            student_courseenrollment
        WHERE
            user_id in ({user_ids})
    '''.format(
        user_ids=','.join(user_enterprise_enrollments.keys())
    )
    lms_data = fetch_lms_data(query)

    user_course_enrollments = defaultdict(list)
    for record in lms_data:
        lms_user_id = str(record['lms_user_id'])
        user_course_enrollments[lms_user_id].append(record)
    logger.info(
        '%s Enterprise learners without enterprise course enrollment but with course enrollment',
        len(user_course_enrollments.keys())
    )

    # How many do not have student_courseenrollment records?
    enterprise_learners_no_enrollments = set(user_enterprise_enrollments.keys()) - set(user_course_enrollments.keys())
    logger.info(
        '%s Enterprise learners with no enrollments',
        len(enterprise_learners_no_enrollments)
    )

    # Export enterprise learners with missing enterprise_enterprisecourseenrollment records to csv

    # When was the enterprise_enterprisecustomeruser record created?
    query = '''
        SELECT
            user_id,
            created
        FROM
            enterprise_enterprisecustomeruser
        WHERE
            user_id in ({user_ids})
    '''.format(
        user_ids=','.join(user_course_enrollments.keys())
    )
    enterprise_customer_users = fetch_lms_data(query)

    enterprise_learners = {}
    course_enrollment_count = 0
    for enterprise_customer_user in enterprise_customer_users:
        lms_user_id = str(enterprise_customer_user['user_id'])
        enterprise_enrollment = user_enterprise_enrollments[lms_user_id][0]
        course_enrollments = user_course_enrollments[lms_user_id]
        course_enrollment_count += len(course_enrollments)
        enterprise_learner = EnterpriseLearner(
            lms_user_id,
            enterprise_enrollment['user_username'],
            enterprise_enrollment['enterprise_name'],
            enterprise_customer_user['created'],
            course_enrollments
        )
        enterprise_learners[lms_user_id] = enterprise_learner

    logger.info(
        '%s Enterprise learners with missing enterprise_enterprisecourseenrollments',
        len(enterprise_learners.keys())
    )
    logger.info('%s course enrollments with missing enterprise course enrollments', course_enrollment_count)
    # export_missing_enterprise_course_enrollments(enterprise_learners.values())

    # Of those Enterprise learners that are enrolled in courses, but do not
    # have enterprise_enterprisecourseenrollment records, what was the creation
    # date of the user account?
    query = '''
        SELECT
            id AS lms_user_id,
            date_joined
        FROM
            auth_user
        WHERE
            id in ({user_ids})
    '''.format(
        user_ids=','.join(user_course_enrollments.keys())
    )
    lms_data = fetch_lms_data(query)

    existing_accounts = []
    enterprise_accounts = []
    for record in lms_data:
        lms_user_id = record['lms_user_id']
        date_joined = record['date_joined']
        # logger.info('%s %s', lms_user_id, date_joined)
        if date_joined >= ENTERPRISE_CUTOFF_DATE:
            enterprise_accounts.append(record)
        else:
            existing_accounts.append(record)
    logger.info('%s existing user accounts', len(existing_accounts))
    logger.info('%s enterprise accounts', len(enterprise_accounts))

    # Do the enrollments have corresponding consent_datasharingconsent records?
    # TODO: Create enterprise_enterprisecourseenrollment records for these enrollments.
    query = '''
        SELECT
            u.id AS lms_user_id,
            dsc.course_id AS course_id,
            dsc.granted AS granted,
            dsc.created AS created
        FROM
            auth_user u,
            consent_datasharingconsent dsc
        WHERE
            dsc.username = u.username AND
            u.id in ({user_ids})
    '''.format(
        user_ids=','.join(user_course_enrollments.keys())
    )
    lms_data = fetch_lms_data(query)

    user_consent = defaultdict(list)
    for record in lms_data:
        lms_user_id = str(record['lms_user_id'])
        user_consent[lms_user_id].append(record)
        log_record(record)
    logger.info(
        '%s Enterprise learners with a course enrollment and no correpsonding enterprise enrollment have a consent record',
        len(user_consent.keys())
    )

    # Enterprise learners with course enrollment and declined DSC
    vertica_data = fetch_vertica_data(
        VERTICA_QUERY.format(
            where=(
                query_filter +
                ' AND course_id is NOT NULL' +
                ' AND consent_granted = 0'
            )
        )
    )

    # for record in vertica_data:
    #     log_record(record)
    logger.info('%s Enterprise course enrollments with declined DSC', len(vertica_data))

    # Enterprise learners with course enrollment but no data sharing consent
    vertica_data = fetch_vertica_data(
        VERTICA_QUERY.format(
            where=(
                query_filter +
                ' AND course_id is NOT NULL' +
                ' AND consent_granted is NULL'
            )
        )
    )

    user_courses = []
    for record in vertica_data:
        log_record(record)
        lms_user_id = str(record['lms_user_id'])
        course_id = record['course_id']
        user_courses.append("({}, '{}')".format(lms_user_id, course_id))
    logger.info('%s Enterprise course enrollments with NULL DSC', len(vertica_data))

    query = '''
        SELECT
            user_id AS lms_user_id,
            course_id,
            created,
            is_active,
            mode
        FROM
            student_courseenrollment
        WHERE
            (user_id, course_id) IN ({user_courses})
    '''.format(
        user_courses=','.join(user_courses)
    )
    # lms_data = fetch_lms_data(query)
    # logger.info(
    #     '%s Enterprise course enrollments with NULL DSC and a course enrollment',
    #     len(lms_data)
    # )

    # Enterprise learners with no email
    # vertica_data = fetch_vertica_data(
    #     VERTICA_QUERY.format(where=(query_filter + ' AND user_email is NULL'))
    # )
    # logger.info('******** Enterprise learners with no email ********')
    # for record in vertica_data:
    #     log_record(record)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--enterprise-customer', required=False, type=str,
                        help="EnterpriseCustomer UUID.")
    args = parser.parse_args()

    validate_enterprise_data(args.enterprise_customer)

    sys.exit(0)
