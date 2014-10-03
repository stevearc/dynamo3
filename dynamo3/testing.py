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
import logging
import nose
import shutil
import six
import subprocess
import tarfile
import tempfile
from contextlib import closing
from six.moves.urllib.request import urlretrieve  # pylint: disable=F0401,E0611

import locale
import os
from . import DynamoDBConnection


DYNAMO_LOCAL = 'http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest'


class DynamoLocalPlugin(nose.plugins.Plugin):

    """
    Nose plugin to run the Dynamo Local service

    See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html

    """
    name = 'dynamo'

    def __init__(self):
        super(DynamoLocalPlugin, self).__init__()
        self._dynamo_local = None
        self._dynamo = None
        self.port = None
        self.path = None
        self.link = None
        self.region = None

    def options(self, parser, env):
        super(DynamoLocalPlugin, self).options(parser, env)
        parser.add_option('--dynamo-port', type=int, default=8000,
                          help="Run the DynamoDB Local service on this port "
                          "(default 8000)")
        default_path = os.path.join(tempfile.gettempdir(), 'dynamolocal')
        parser.add_option('--dynamo-path', default=default_path,
                          help="Download the Dynamo Local server to this "
                          "directory (default '%s')" % default_path)
        parser.add_option('--dynamo-link', default=DYNAMO_LOCAL,
                          help="The link to the dynamodb local server code "
                          "(default '%s')" % DYNAMO_LOCAL)
        default_region = 'us-east-1'
        parser.add_option('--dynamo-region', default=default_region,
                          help="Connect to this AWS region (default %s)" %
                          default_region)
        parser.add_option('--dynamo-live', action='store_true',
                          help="Run tests on ACTUAL DynamoDB region. "
                          "Standard AWS charges apply. "
                          "This will destroy all tables you have in the "
                          "region.")

    def configure(self, options, conf):
        super(DynamoLocalPlugin, self).configure(options, conf)
        self.port = options.dynamo_port
        self.path = options.dynamo_path
        self.link = options.dynamo_link
        self.region = options.dynamo_region
        self.live = options.dynamo_live
        logging.getLogger('botocore').setLevel(logging.WARNING)

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
                    with closing(tarfile.open(tarball, 'r:gz')) as archive:
                        archive.extractall(self.path)
                    os.unlink(tarball)

                # Run the jar
                lib_path = os.path.join(self.path, 'DynamoDBLocal_lib')
                jar_path = os.path.join(self.path, 'DynamoDBLocal.jar')
                cmd = ['java', '-Djava.library.path=' + lib_path, '-jar',
                       jar_path, '--port', str(self.port), '--inMemory']
                self._dynamo_local = subprocess.Popen(cmd,
                                                      stdout=subprocess.PIPE,
                                                      stderr=subprocess.STDOUT)
                self._dynamo = DynamoDBConnection.connect(
                    self.region, access_key='', secret_key='',
                    host='localhost', port=self.port, is_secure=False)
        return self._dynamo

    def startContext(self, context):  # pylint: disable=C0103
        """ Called at the beginning of modules and TestCases """
        # If this is a TestCase, dynamically set the dynamo connection
        if inspect.isclass(context) and hasattr(context, 'dynamo'):
            context.dynamo = self.dynamo

    def finalize(self, result):
        """ terminate the dynamo local service """
        if self._dynamo_local is not None:
            self._dynamo_local.terminate()
            if not result.wasSuccessful():  # pragma: no cover
                output = self._dynamo_local.stdout.read()
                encoding = locale.getdefaultlocale()[1] or 'utf-8'
                six.print_("DynamoDB Local output:")
                six.print_(output.decode(encoding))
