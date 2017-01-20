Changelog
=========

0.4.10
------
* Fixed DynamoDB Local link in testing framework

0.4.9
-----
* Feature: Result objects from get_item have an ``exists`` flag
* Feature: ``wait`` keyword for create and delete table

0.4.8
-----
* Bug fix: Scans/Queries could return incomplete results if AWS returned an empty Items list

0.4.7
-----
* New ``RateLimit`` class to avoid blowing through your provisioned throughput

0.4.6
-----
* New ``Limit`` class for more complex query limit behavior
* Bug fix: Scan and Query with ``Select='COUNT'`` will page results properly

0.4.5
-----
* batch_get supports ``alias`` arg for ExpressionAttributeNames

0.4.4
-----
* Make connection stateless again. Puts consumed_capacity into response object and fixes mystery crash.

0.4.3
-----
* Bug fix: getting ConsumedCapacity doesn't crash for BatchGetItem and BatchWriteItem
* Feature: connection.default_return_capacity
* Feature: hooks for ``precall``, ``postcall``, and ``capacity``
* Better handling of ConsumedCapacity results

0.4.2
-----
* Feature: New methods to take advantage of the newer expression API. See get_item2, put_item2.
* Feature: Shortcut ``use_version`` for switching over to the new APIs.

0.4.1
-----
* Feature: update_table can create and delete global indexes
* Feature: New methods to take advantage of the newer expression API. See scan2, query2, update_item2, and delete_item2.

0.4.0
-----
* Migrating to botocore client API since services will be deprecated soon

0.3.2
-----
* Bug fix: Serialization of blobs broken with botocore 0.85.0

0.3.1
-----
* Bug fix: Crash when parsing description of table being deleted

0.3.0
-----
* **Breakage**: Dropping support for python 3.2 due to lack of botocore support
* Feature: Support JSON document data types

Features thanks to DynamoDB upgrades: https://aws.amazon.com/blogs/aws/dynamodb-update-json-and-more/

0.2.2
-----
* Tweak: Nose plugin allows setting region when connecting to DynamoDB Local

0.2.1
-----
* Feature: New, unified ``connect`` method

0.2.0
-----
* Feature: More expressive 'expected' conditionals
* Feature: Queries can filter on non-indexed fields
* Feature: Filter constraints may be OR'd together

Features thanks to DynamoDB upgrades: http://aws.amazon.com/blogs/aws/improved-queries-and-updates-for-dynamodb/

0.1.3
-----
* Bug fix: sometimes crash after deleting table
* Bug fix: DynamoDB Local nose plugin fails

0.1.2
-----
* Bug fix: serializing ints fails

0.1.1
-----
* Feature: Allow ``access_key`` and ``secret_key`` to be passed to the ``DynamoDBConnection.connect_to_*`` methods

0.1.0
-----
* First public release
