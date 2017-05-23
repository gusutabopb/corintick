import logging
import os

import yaml

logger = logging.getLogger('pytrthree')


def make_logger(name, config) -> logging.Logger:
    if not os.path.exists(config['log']):
        os.makedirs(config['log'])
    fname = os.path.join(config['log'], f'{name}.log')
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