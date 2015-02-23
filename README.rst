Dynamo3
=======
:Build: |build|_ |coverage|_
:Downloads: http://pypi.python.org/pypi/dynamo3
:Source: https://github.com/stevearc/dynamo3

.. |build| image:: https://travis-ci.org/stevearc/dynamo3.png?branch=master
.. _build: https://travis-ci.org/stevearc/dynamo3
.. |coverage| image:: https://coveralls.io/repos/stevearc/dynamo3/badge.png?branch=master
.. _coverage: https://coveralls.io/r/stevearc/dynamo3?branch=master

This is a stop-gap measure while `boto3 <http://github.com/boto/boto3>`_ is
under development. I have kept the API mostly similar to boto (with a few
improvements). This should only be used if you need to connect to DynamoDB
using Python 3. It was created for `dql <http://github.com/mathcamp/dql>`_ and
`flywheel <http://github.com/mathcamp/flywheel>`_.
