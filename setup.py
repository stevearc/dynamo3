""" Setup file """
import os
import sys

from setuptools import find_packages, setup

HERE = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(HERE, "README.rst")) as f:
    README = f.read()

with open(os.path.join(HERE, "CHANGES.rst")) as f:
    CHANGES = f.read()

REQUIREMENTS_TEST = open(os.path.join(HERE, "requirements_test.txt")).readlines()

REQUIREMENTS = [
    "botocore>=0.89.0",
]

if __name__ == "__main__":
    setup(
        name="dynamo3",
        version="1.0.0",
        description="Python 3 compatible library for DynamoDB",
        long_description=README + "\n\n" + CHANGES,
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
        ],
        author="Steven Arcangeli",
        author_email="stevearc@stevearc.com",
        url="http://github.com/stevearc/dynamo3",
        keywords="aws dynamo dynamodb",
        include_package_data=True,
        packages=find_packages(exclude=("tests",)),
        license="MIT",
        entry_points={
            "console_scripts": [
                "dynamodb-local = dynamo3.testing:run_dynamo_local",
            ],
            "nose.plugins": [
                "dynamolocal=dynamo3.testing:DynamoLocalPlugin",
            ],
        },
        python_requires=">=3.6",
        install_requires=REQUIREMENTS,
        tests_require=REQUIREMENTS + REQUIREMENTS_TEST,
    )
