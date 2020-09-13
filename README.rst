Dynamo3
=======
:Build: |build|_ |coverage|_
:Downloads: http://pypi.python.org/pypi/dynamo3
:Source: https://github.com/stevearc/dynamo3

.. |build| image:: https://travis-ci.org/stevearc/dynamo3.png?branch=master
.. _build: https://travis-ci.org/stevearc/dynamo3
.. |coverage| image:: https://coveralls.io/repos/stevearc/dynamo3/badge.png?branch=master
.. _coverage: https://coveralls.io/r/stevearc/dynamo3?branch=master

Dynamo3 is a library for querying DynamoDB. It is designed to be higher-level
than boto (it's built on top of botocore), to make simple operations easier to
perform and understand.

Features
--------
* Mypy-typed API
* Python object wrappers for most AWS data structures
* Automatic serialization of built-in types, with hooks for custom types
* Automatic paging of results
* Automatic batching for batch_write_item
* Exponential backoff of requests when throughput is exceeded
* Throughput limits to self-throttle requests to a certain rate
* Nose plugin for running DynamoDB Local

DynamoDB features that are not yet supported
--------------------------------------------
* Reading from streams
* Adding/removing tags on a table
* Table backups
* Scanning with segments
* Table replicas (Global tables version 2019.11.21)
* Table auto scaling
* DAX

Note that you can still access these APIs by using ``DynamoDBConnection.call``,
though you may prefer to go straight to boto3/botocore.
