""" Testing tools for DynamoDB """
import botocore.session
import inspect
import logging
import nose
import shutil
import six
import subprocess
import tempfile
from six.moves.urllib.request import urlretrieve

import locale
import os
from . import DynamoDBConnection


DYNAMO_LOCAL = 'https://s3-us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_2014-01-08.tar.gz'

class DynamoLocalPlugin(nose.plugins.Plugin):

    """
    Nose plugin to run the Dynamo Local service

    See: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.html

    """
    name = 'dynamolocal'

    def __init__(self):
        super(DynamoLocalPlugin, self).__init__()
        self._dynamo_local = None
        self._dynamo = None
        self.port = None
        self.path = None
        self.link = None

    def options(self, parser, env):
        super(DynamoLocalPlugin, self).options(parser, env)
        parser.add_option('--dynamo-port', type=int, default=8000,
                          help="Run the Dynamo Local service on this port "
                          "(default %(default)s)")
        parser.add_option('--dynamo-path', help="Download the Dynamo Local "
                          "server to this directory")
        parser.add_option('--dynamo-link', default=DYNAMO_LOCAL,
                          help="The link to the dynamodb local server code "
                          "(default %(default)s)")

    def configure(self, options, conf):
        super(DynamoLocalPlugin, self).configure(options, conf)
        self.port = options.dynamo_port
        self.path = options.dynamo_path
        self.link = options.dynamo_link
        if self.path is None:
            self.path = os.path.join(tempfile.gettempdir(), 'dynamolocal')
        logging.getLogger('botocore').setLevel(logging.WARNING)

    @property
    def dynamo(self):
        """ Lazy loading of the dynamo connection """
        if self._dynamo is None:
            if not os.path.exists(self.path):
                tarball = urlretrieve(self.link)[0]
                subprocess.check_call(['tar', '-zxf', tarball])
                name = os.path.basename(self.link).split('.')[0]
                shutil.move(name, self.path)
                os.unlink(tarball)

            lib_path = os.path.join(self.path, 'DynamoDBLocal_lib')
            jar_path = os.path.join(self.path, 'DynamoDBLocal.jar')
            cmd = ['java', '-Djava.library.path=' + lib_path, '-jar', jar_path,
                   '--port', str(self.port)]
            self._dynamo_local = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                                  stderr=subprocess.STDOUT)
            session = botocore.session.get_session()
            self._dynamo = DynamoDBConnection.connect_to_host(port=self.port,
                                                              session=session)
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
            if not result.wasSuccessful(): # pragma: no cover
                output = self._dynamo_local.stdout.read()
                encoding = locale.getdefaultlocale()[1] or 'utf-8'
                six.print_("DynamoDB Local output:")
                six.print_(output.decode(encoding))
