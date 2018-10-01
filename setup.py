from setuptools import setup, find_packages

setup(
    name="sqlalchemy_query_helper",
    version="0.0.1",
    packages=find_packages(),
    install_requires=['SQLAlchemy>=1.2.12', 'python-dateutil>=2.7.3'],
    author='Edem Tsalah',
    author_email='edem.tsalah@gmail.com',
    description=(
        'A library that helps to query data from an sql database as '
        'though it was from a mongodb database'
    ),
    license='MIT'
)
