#!/usr/bin/python

import argparse
import importlib
import time
import os
import sys
import json
import logging
import logging.config

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark

__author__ = 'kb'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    print ("Called with arguments: %s" % args)

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else '',
        'PYSPARK_JOB_DIR': os.path.join('./jobs', args.job_name)
    }

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        print ('job_args_tuples: %s' % job_args_tuples)
        job_args = {a[0]: a[1] for a in job_args_tuples}

    print ('\nRunning job %s...\nenvironment is %s\n' % (args.job_name, environment))

    os.environ.update(environment)
    sc = pyspark.SparkContext(appName=args.job_name, environment=environment)
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    # get logging config file
    with open('./logging.json') as log_json:
        logging_config = json.load(log_json)
    logging.config.dictConfig(logging_config)
    logger_main = logging.getLogger(__name__)

    # to test various levels
    # logger_main.debug('debug message')
    # logger_main.info('info message')
    # logger_main.warn('warn message')
    # logger_main.error('error message')
    # logger_main.critical('critical message')

    # initialize log4j for yarn cluster logs
    log4jLogger = sc._jvm.org.apache.log4j
    logger_pyspark = log4jLogger.LogManager.getLogger(__name__)
    logger_pyspark.info("pyspark script logger initialized")

    # push logs to hdfs
    # logpath = job_config['logger_config']['path']
    # get the hdfs directory where logs are to be stored
    # hdfs = HDFileSystem(host=job_config['HDFS_host'], port=port)
    # current_job_logpath = logpath + f"{datetime.datetime.now():%Y-%m-%d}"
    # job_run_path = current_job_logpath + '/' + str(int(f"{datetime.datetime.now():%H}") - 2) + '/'

    # if hdfs.exists(current_job_logpath):
    #     pass
    # else:
    #     # create directory for today
    #     hdfs.mkdir(current_job_logpath)
    #     hdfs.chmod(current_job_logpath, mode=0o777)

    # hdfs.put("info.log", job_run_path + "info.log")
    # hdfs.put("errors.log", job_run_path + "errors.log")
    # hdfs.put("spark_job_log4j.log", job_run_path + "spark_job_log4j.log")


    start = time.time()
    job_module.analyze(sc, **job_args)
    end = time.time()

    print ("\nExecution of job %s took %s seconds" % (args.job_name, end-start))
