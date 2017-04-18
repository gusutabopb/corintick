import logging
import os

import yaml

LOG_DIR = os.path.expanduser('~/plugaai/_logs')


def make_logger(name, fname=None) -> logging.Logger:
    if fname is None:
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        fname = os.path.join(LOG_DIR, f'{name}.log')
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s: %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)

    file_handler = logging.FileHandler(filename=fname, mode='a')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

logger = make_logger(__name__)


def load_config(config_path):
    if config_path is None:
        return {'host': {},
                'database': {'name': 'corintick'},
                'buckets': ['corintick']}
    config = yaml.load(open(os.path.expanduser(config_path)))
    config_keys = {'host', 'database', 'buckets'}
    if config_keys - set(config.keys()):
        raise ValueError(f'Config keys missing: {config_keys - set(config.keys())}')
    else:
        return config
