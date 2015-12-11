""" Setup file """
import os
import sys

from setuptools import setup, find_packages


HERE = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(HERE, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(HERE, 'CHANGES.rst')) as f:
    CHANGES = f.read()

REQUIREMENTS = [
    'botocore>=0.89.0',
    'six',
]

TEST_REQUIREMENTS = [
    'nose',
    'mock',
]

if sys.version_info[:2] < (2, 7):
    TEST_REQUIREMENTS.append('unittest2')

if __name__ == "__main__":
    setup(
        name='dynamo3',
        version='0.4.6',
        description='Python 3 compatible library for DynamoDB',
        long_description=README + '\n\n' + CHANGES,
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
        ],
        author='Steven Arcangeli',
        author_email='stevearc@stevearc.com',
        url='http://github.com/stevearc/dynamo3',
        keywords='aws dynamo dynamodb',
        include_package_data=True,
        packages=find_packages(exclude=('tests',)),
        license='MIT',
        entry_points={
            'nose.plugins': [
                'dynamolocal=dynamo3.testing:DynamoLocalPlugin',
            ],
        },
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + TEST_REQUIREMENTS,
    )
