from setuptools import setup, find_packages

setup(
    name='dbmigrator',
    version='0.1.0',
    packages=find_packages(include=['app', 'app.*']),
    install_requires=[
        'APScheduler>=3.6.3',
        'Flask>=1.1.2',
        'Flask-APScheduler>=1.11.0',
        'pandas>=1.0.3',
        'psycopg2>=2.8.5',
        'pymongo>=3.10.1',
        'SQLAlchemy>=1.3.17'
    ]
)