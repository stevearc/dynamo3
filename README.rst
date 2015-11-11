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
perform and understand. It has some nice features like exponential backoff and
automatic pagination built in.
