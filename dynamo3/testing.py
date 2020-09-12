"""
Testing tools for DynamoDB

To use the DynamoDB Local service in your unit tests, enable the nose plugin by
running with '--with-dynamo'. Your tests can access the
:class:`~dynamo3.DynamoDBConnection` object by putting a 'dynamo' attribute on
the test class. For example::

    class TestDynamo(unittest.TestCase):
        dynamo = None

        def test_delete_table(self):
            self.dynamo.delete_table('foobar')

The 'dynamo' object will be detected and replaced by the nose plugin at
runtime.

"""
import inspect
import locale
import logging
import os
import subprocess
import tarfile
import tempfile
from contextlib import closing
from typing import Optional
from urllib.request import urlretrieve

import nose

from . import DynamoDBConnection

DYNAMO_LOCAL = (
    "https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz"
)

DEFAULT_REGION = "us-east-1"
DEFAULT_PORT = 8000


class DynamoLocalPlugin(nose.plugins.Plugin):

    """
    Nose plugin to run the Dynamo Local service

    See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html

    """

    name = "dynamo"

    def __init__(self):
        super(DynamoLocalPlugin, self).__init__()
        self._dynamo_local: Optional[subprocess.Popen] = None
        self._dynamo: Optional[DynamoDBConnection] = None
        self.port: int = DEFAULT_PORT
        self.path: str = ""
        self.link: str = ""
        self.region: str = DEFAULT_REGION
        self.live: bool = False

    def options(self, parser, env):
        super(DynamoLocalPlugin, self).options(parser, env)
        parser.add_option(
            "--dynamo-port",
            type=int,
            default=DEFAULT_PORT,
            help="Run the DynamoDB Local service on this port " "(default %(default)s)",
        )
        default_path = os.path.join(tempfile.gettempdir(), "dynamolocal")
        parser.add_option(
            "--dynamo-path",
            default=default_path,
            help="Download the Dynamo Local server to this "
            "directory (default '%(default)s')",
        )
        parser.add_option(
            "--dynamo-link",
            default=DYNAMO_LOCAL,
            help="The link to the dynamodb local server code "
            "(default '%(default)s')",
        )
        parser.add_option(
            "--dynamo-region",
            default=DEFAULT_REGION,
            help="Connect to this AWS region (default %(default)s)",
        )
        parser.add_option(
            "--dynamo-live",
            action="store_true",
            help="Run tests on ACTUAL DynamoDB region. "
            "Standard AWS charges apply. "
            "This will destroy all tables you have in the "
            "region.",
        )

    def configure(self, options, conf):
        super(DynamoLocalPlugin, self).configure(options, conf)
        self.port = options.dynamo_port
        self.path = options.dynamo_path
        self.link = options.dynamo_link
        self.region = options.dynamo_region
        self.live = options.dynamo_live
        logging.getLogger("botocore").setLevel(logging.WARNING)

    @property
    def dynamo(self):
        """ Lazy loading of the dynamo connection """
        if self._dynamo is None:
            if self.live:  # pragma: no cover
                # Connect to live DynamoDB Region
                self._dynamo = DynamoDBConnection.connect(self.region)
            else:
                # Download DynamoDB Local
                if not os.path.exists(self.path):
                    tarball = urlretrieve(self.link)[0]
                    with closing(tarfile.open(tarball, "r:gz")) as archive:
                        archive.extractall(self.path)
                    os.unlink(tarball)

                # Run the jar
                lib_path = os.path.join(self.path, "DynamoDBLocal_lib")
                jar_path = os.path.join(self.path, "DynamoDBLocal.jar")
                cmd = [
                    "java",
                    "-Djava.library.path=" + lib_path,
                    "-jar",
                    jar_path,
                    "--port",
                    str(self.port),
                    "--inMemory",
                ]
                self._dynamo_local = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
                )
                self._dynamo = DynamoDBConnection.connect(
                    self.region,
                    access_key="",
                    secret_key="",
                    host="localhost",
                    port=self.port,
                    is_secure=False,
                )
        return self._dynamo

    def startContext(self, context):  # pylint: disable=C0103
        """ Called at the beginning of modules and TestCases """
        # If this is a TestCase, dynamically set the dynamo connection
        if inspect.isclass(context) and hasattr(context, "dynamo"):
            context.dynamo = self.dynamo

    def finalize(self, result):
        """ terminate the dynamo local service """
        if self._dynamo_local is not None:
            self._dynamo_local.terminate()
            if (
                not result.wasSuccessful() and self._dynamo_local.stdout is not None
            ):  # pragma: no cover
                output = self._dynamo_local.stdout.read()
                encoding = locale.getdefaultlocale()[1] or "utf-8"
                print("DynamoDB Local output:")
                print(output.decode(encoding))
