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
import argparse
import inspect
import locale
import logging
import os
import subprocess
import tarfile
import tempfile
from contextlib import closing
from typing import List, Optional
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
                cmd = _dynamo_local_cmd(self.path, self.link, self.port, in_memory=True)
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


def run_dynamo_local(argv=None):
    """ Run DynamoDB Local """
    parser = argparse.ArgumentParser(description=run_dynamo_local.__doc__)
    default_path = os.path.join(tempfile.gettempdir(), "dynamolocal")
    parser.add_argument(
        "--path",
        help="Path to download DynamoDB local (default %(default)s)",
        default=default_path,
    )
    parser.add_argument(
        "--link",
        help="URL to use to download DynamoDB local (default %(default)s)",
        default=DYNAMO_LOCAL,
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="Port to listen on (default %(default)d)",
        default=DEFAULT_PORT,
    )
    parser.add_argument(
        "--inMemory",
        action="store_true",
        help="When specified, DynamoDB Local will run in memory.",
    )
    parser.add_argument(
        "--cors",
        help="Enable CORS support for javascript against a specific allow-list list the domains separated by , use '*' for public access (default is '*')",
    )
    parser.add_argument(
        "--dbPath",
        help="Specify the location of your database file.  Default is the current directory",
    )
    parser.add_argument(
        "--optimizeDbBeforeStartup",
        action="store_true",
        help="Optimize the underlying backing store database tables before starting up the server",
    )
    parser.add_argument(
        "--sharedDb",
        action="store_true",
        help="When specified, DynamoDB Local will use a single database instead of separate databases for each credential and region. As a result, all clients will interact with the same set of tables, regardless of their region and credential configuration. (Useful for interacting with Local through the JS Shell in addition to other SDKs)",
    )
    parser.add_argument(
        "--delayTransientStatuses",
        action="store_true",
        help="When specified, DynamoDB Local will introduce delays to hold various transient table and index statuses so that it simulates actual service more closely.",
    )

    args = parser.parse_args(argv)
    cmd = _dynamo_local_cmd(
        args.path,
        args.link,
        args.port,
        args.inMemory,
        args.cors,
        args.dbPath,
        args.optimizeDbBeforeStartup,
        args.delayTransientStatuses,
        args.sharedDb,
    )
    subprocess.call(cmd)


def _dynamo_local_cmd(
    path: str,
    link: str,
    port: int,
    in_memory: bool = False,
    cors: Optional[str] = None,
    db_path: Optional[str] = None,
    optimize_before_startup: bool = False,
    delay_transient_statuses: bool = False,
    shared_db: bool = False,
) -> List[str]:
    # Download DynamoDB Local
    if not os.path.exists(path):
        tarball = urlretrieve(link)[0]
        with closing(tarfile.open(tarball, "r:gz")) as archive:
            archive.extractall(path)
        os.unlink(tarball)

    # Run the jar
    lib_path = os.path.join(path, "DynamoDBLocal_lib")
    jar_path = os.path.join(path, "DynamoDBLocal.jar")
    cmd = [
        "java",
        "-Djava.library.path=" + lib_path,
        "-jar",
        jar_path,
        "--port",
        str(port),
    ]
    if in_memory:
        cmd.append("-inMemory")
    if cors is not None:
        cmd.extend(["-cors", cors])
    if db_path is not None:
        cmd.extend(["-dbPath", db_path])
    if optimize_before_startup:
        cmd.append("-optimizeDbBeforeStartup")
    if delay_transient_statuses:
        cmd.append("-delayTransientStatuses")
    if shared_db:
        cmd.append("-sharedDb")
    return cmd
