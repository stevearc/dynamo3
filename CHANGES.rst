Changelog
=========
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
