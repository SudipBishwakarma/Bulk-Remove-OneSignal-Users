import pandas as pd
import numpy as np
import requests

import os
import shutil
from time import time, strftime, gmtime
from multiprocessing import Queue, Process
import logging
from logging import handlers, config
import threading

from settings import *

BASE_DIR = os.getcwd()
FILE_PATH = os.path.join(BASE_DIR, f'{FILE_NAME}.csv')
TMP_DIR = os.path.join(BASE_DIR, 'tmp')


def create_dir(directory):
    """Create temporary directory to store generated files."""
    try:
        if not os.path.exists(directory):
            os.mkdir(directory)
            print('Temporary directory created successfully.')
    except OSError as e:
        print(e)


def remove_dir(directory):
    """Remove temporary directory"""
    try:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            print('Temporary directory deleted successfully.')
    except OSError as e:
        print(e)


def temp_csv(i):
    """Write data chunks to temporary files"""
    start = CHUNK_SIZE * i
    end = CHUNK_SIZE * (i + 1)
    end = len(data) if end > len(data) else end

    data[start:end].to_csv(
        os.path.join(TMP_DIR, f'{FILE_NAME}_{i}.csv'),
        index=False
    )


def worker_process(num, q):
    """Function each process triggers upon instantiation"""
    tmp_file_path = os.path.join(TMP_DIR, f'{FILE_NAME}_{num}.csv')
    tmp_file = np.squeeze(pd.read_csv(tmp_file_path))
    rows = range(tmp_file.count())

    qh = handlers.QueueHandler(q)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(qh)

    def delete_user(row):
        headers = {'Authorization': f'Basic {API_KEY}'}
        params = {'app_id': f'{APP_ID}'}
        url = f'https://onesignal.com/api/v1/players/{tmp_file[row]}'

        success_logger = logging.getLogger('success')        
        error_logger = logging.getLogger('error')
        try:
            response = requests.delete(url, headers=headers, params=params)
            msg = 'ID not found'
            if response.ok:
                msg = 'Deleted'
            success_logger.info(f'{tmp_file[row]} - {msg}')
        except Exception as e:
            error_logger.error(tmp_file[row])

    _delete_user = np.vectorize(delete_user, cache=True)
    _delete_user(rows)


def logger_thread(q):
    while True:
        record = q.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)


if __name__ == '__main__':
    tic = time()
    data = pd.read_csv(FILE_PATH)
    segment = len(data) // CHUNK_SIZE
    segment = segment + 1 if (len(data) % CHUNK_SIZE > 0) else segment
    segment_list = range(segment)

    # Create temporary directory to store generated temporary files.
    create_dir(TMP_DIR)

    _temp_csv = np.vectorize(temp_csv)
    _temp_csv(segment_list)
    print("Temporary files generated.")

    q = Queue()
    logger_config = {
        'version': 1,
        'formatters': {
            'detailed': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(name)-15s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'detailed'
            },
            'success_file': {
                'class': 'logging.FileHandler',
                'filename': 'success.log',
                'mode': 'w',
                'formatter': 'detailed',
                'level': 'INFO'
            },
            'error_file': {
                'class': 'logging.FileHandler',
                'filename': 'error.log',
                'mode': 'w',
                'level': 'ERROR'
            }
        },
        'loggers': {
            'success': {
                'handlers': ['success_file']
            },
            'error': {
                'handlers': ['error_file']
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console']
        }
    }

    processes = []

    for i in segment_list:
        p = Process(target=worker_process, args=(i, q))
        processes.append(p)
        p.start()

    config.dictConfig(logger_config)
    logging_thread = threading.Thread(target=logger_thread, args=(q,))
    logging_thread.start()

    for p in processes:
        p.join()

    q.put(None)

    logging_thread.join()

    # Delete temporary directory along with its files.
    remove_dir(TMP_DIR)

    print("Process complete")
    print('Time spent about', strftime('%Hhr %Mm %Ss', gmtime(time() - tic)))
