#!/usr/bin/env python
from setuptools import setup

with open('README.rst', 'r') as f:
    long_description = f.read()

setup(name='corintick',
      version='0.2.0',
      description='Column-based datastore for historical timeseries',
      long_description=long_description,
      author='Gustavo Bezerra',
      author_email='gusutabopb@gmail.com',
      url='https://github.com/plugaai/corintick',
      packages=['corintick'],
      python_requires='>=3.6',
      license='GPL',
      install_requires=[
          'lz4>=1.0.0',
          'pandas>=0.23',
          'pymongo>=3.6',
          'numpy',
          'pytz',
          'msgpack-python'
      ],
      extras_require={
          'test': [
              'pytest',
              'pytest-cov',
              'flake8',
          ]
      },
      classifiers=[
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'Intended Audience :: Financial and Insurance Industry',
          'Development Status :: 4 - Beta',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          "Topic :: Database",
          "Topic :: Database :: Front-Ends",
          "Topic :: Software Development :: Libraries",
      ])
