import logging
import os
from datetime import datetime


def init_logger(
        name='logger',
        file_log=False,
        single_date=True,
        rotate=False,
):

    formatter = logging.Formatter(fmt=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')

    if file_log:
        log_dir = "logs"
        if not os.path.isdir(log_dir):
            try:
                os.system(f'mkdir {log_dir}')
            except Exception as e:
                print(f'failed to create log directory on a path::{log_dir}::{e}')
                exit()

        if not single_date and rotate:
            fn = f"{name}.log"
        elif single_date and not rotate:
            fn = f"{name}_{str(datetime.now()).split(' ')[0]}.log"
        else:
            fn = f"{name}_{str(datetime.now()).replace(' ', '_').replace(':', '_')}.log"

        log_path = os.path.join(log_dir, fn)

        handler = logging.FileHandler(log_path)
        handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear() if logger.hasHandlers() else None

    logger.addHandler(handler)
    return logger
