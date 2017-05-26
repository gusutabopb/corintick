import logging
import os

import yaml

logger = logging.getLogger('pytrthree')


def make_logger(name, config=None) -> logging.Logger:
    log_path = os.path.expanduser(config['log']) if config else os.getcwd()
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    fname = os.path.join(log_path, f'{name}.log')
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
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


def load_config(config_path):
    if config_path is None:
        return {'host': {},
                'database': {'name': 'corintick'},
                'collections': ['corintick']}
    config = yaml.load(open(os.path.expanduser(config_path)))
    config_keys = {'host', 'database', 'collections'}
    if config_keys - set(config.keys()):
        raise ValueError(f'Config keys missing: {config_keys - set(config.keys())}')
    else:
        return config