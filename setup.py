from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(name='pg_partitioner',
      version=version,
      description="Scripts for partioning PostgreSQL tables",
      long_description="""\
Handle automatic generation of range based partition tables including creation of identical indexes and constraints (fkeys are optional), insert triggers for the parent tables, and optionally moves data from parent table into child tables.""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='postgresql psycopg2 database partitioning',
      author='Erik Jones',
      author_email='mage2k@gmail.com',
      url='',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'psycopg2'
      ],
      entry_points={  
          "console_scripts": [  
              'pg_partitioner = pg_partitioner.pg_partitioner:main'  
          ]},
      )
