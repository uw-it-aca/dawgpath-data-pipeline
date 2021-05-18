import os
from setuptools import setup

README = """
See the README on `GitHub
<https://github.com/uw-it-aca/prereq-data-pipeline>`_.
"""

# The VERSION file is created by travis-ci, based on the tag name
version_path = 'prereq_data_pipeline/VERSION'
VERSION = open(os.path.join(os.path.dirname(__file__), version_path)).read()
VERSION = VERSION.replace("\n", "")

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

url = "https://github.com/uw-it-aca/prereq-data-pipeline"
setup(
    name='Prereq Data Pipeline',
    version=VERSION,
    packages=['prereq_data_pipeline'],
    author="UW-IT AXDD",
    author_email="aca-it@uw.edu",
    include_package_data=True,
    install_requires=[
        'pandas~=1.1.5',
        'pyodbc==4.0.30',
        'SQLAlchemy~=1.3.23',
        'SQLAlchemy-Utils~=0.37.2',
        'commonconf~=1.1'
    ],
    license='',
    description='A tool for managing prereq map data',
    long_description=README,
    url=url,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
    ],
)
