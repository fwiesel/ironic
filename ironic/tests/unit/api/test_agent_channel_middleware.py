#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Exercise channel requests through the configured application.

Basic authentication uses the real middleware with a local fixture file.
The Keystone credential middleware is a local rejecting stand-in. The
application, public route selector, policy layer, controller, and journal
are real. These tests make no requests to a Keystone service.
"""

import hashlib
from http import client as http_client
import json
import os
from unittest import mock

import bcrypt
import ddt
import fixtures
from oslo_policy import policy as oslo_policy
from oslo_utils import uuidutils
import pecan
import webob
import webtest

from ironic.api import app as api_app
from ironic.api.controllers import base as api_base
from ironic.common import policy
from ironic.tests.unit.db import base as db_base
from ironic.tests.unit.db import utils as db_utils

TOKEN_HEADER = 'X-Ironic-Agent-Token'
TOKEN = 'middleware-test-agent-token'
WRONG_TOKEN = 'middleware-test-wrong-token'
VERIFIER = hashlib.sha256(TOKEN.encode('utf-8')).hexdigest()


@ddt.ddt
class AgentChannelMiddlewareTestCase(db_base.DbTestCase):

    def setUp(self):
        super().setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), 1,
            self.conductor.id, 'middleware-test-incarnation',
            VERIFIER, 'agent-poll')
        self.command = self.dbapi.publish_agent_command(
            self.session['uuid'], 1, 'middleware-test-command', 'power_on',
            {}, 'completion', self.conductor.id,
            'middleware-test-incarnation', self.session['fence'])
        self.credential_requests = []
        self.addCleanup(pecan.set_config, {}, overwrite=True)

    def _make_app(self, strategy):
        self.config(auth_strategy=strategy)
        if strategy == 'http_basic':
            directory = self.useFixture(fixtures.TempDir()).path
            filename = os.path.join(directory, 'test-basic-users')
            digest = bcrypt.hashpw(b'fixture-password', bcrypt.gensalt(4))
            with open(filename, 'w') as stream:
                stream.write('fixture-user:%s\n' % digest.decode('ascii'))
            self.config(http_basic_auth_user_file=filename)
        else:
            def rejecting_credentials(environ, start_response):
                self.credential_requests.append(environ['PATH_INFO'])
                return webob.Response(
                    status=http_client.UNAUTHORIZED,
                    headers={'X-Test-Credential-Middleware': 'keystone'})(
                        environ, start_response)

            patcher = mock.patch.object(
                api_app.auth_token, 'AuthProtocol', autospec=True,
                return_value=rejecting_credentials)
            patcher.start()
            self.addCleanup(patcher.stop)
        return webtest.TestApp(api_app.setup_app(
            pecan_config=api_app.get_pecan_config()))

    def _headers(self, token=TOKEN):
        headers = {api_base.Version.string: '1.101'}
        if token is not None:
            headers[TOKEN_HEADER] = token
        return headers

    def _post(self, app, action, token=TOKEN):
        if action == 'work':
            body = {'session': self.session['uuid'],
                    'execution': self.session['execution_id'],
                    'workflow_epoch': 1, 'poll_id': 'middleware-test-poll',
                    'wait_seconds': 0}
        else:
            body = [{'command_id': self.command['uuid'],
                     'workflow_epoch': 1, 'kind': 'result',
                     'payload': {'ok': True}}]
        return app.post_json(
            '/v1/agent_channel/%s/%s' % (self.node.uuid, action), body,
            headers=self._headers(token), expect_errors=True)

    def _assert_credential_bypass(self, response):
        self.assertEqual([], self.credential_requests)
        self.assertNotIn('X-Test-Credential-Middleware', response.headers)
        self.assertNotIn('WWW-Authenticate', response.headers)

    @ddt.data('http_basic', 'keystone')
    def test_existing_public_version_route_reaches_the_application(self,
                                                                   strategy):
        app = self._make_app(strategy)
        response = app.get('/v1', headers=self._headers(), expect_errors=True)
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual('v1', response.json['id'])
        self._assert_credential_bypass(response)

    @ddt.data('http_basic', 'keystone')
    def test_work_uses_only_the_agent_credential(self, strategy):
        response = self._post(self._make_app(strategy), 'work')
        self.assertEqual(http_client.OK, response.status_int)
        self._assert_credential_bypass(response)
        self.assertEqual('command', response.json['outcome'])
        self.assertEqual(self.command['uuid'],
                         response.json['command']['uuid'])
        self.assertEqual(self.command, self.dbapi.get_agent_channel_command(
            self.command['uuid']))

    @ddt.data('http_basic', 'keystone')
    def test_reports_uses_only_the_agent_credential(self, strategy):
        response = self._post(self._make_app(strategy), 'reports')
        self.assertEqual(http_client.OK, response.status_int)
        self._assert_credential_bypass(response)
        self.assertEqual([{'command_id': self.command['uuid'],
                           'kind': 'result',
                           'disposition': 'acknowledged'}],
                         response.json['reports'])
        self.assertEqual({'ok': True}, self.dbapi.get_agent_channel_command(
            self.command['uuid'])['result'])

    @ddt.data('http_basic', 'keystone')
    def test_empty_reports_batch_uses_the_agent_credential(self, strategy):
        app = self._make_app(strategy)
        response = app.post_json(
            '/v1/agent_channel/%s/reports' % self.node.uuid, [],
            headers=self._headers(), expect_errors=True)
        self.assertEqual(http_client.OK, response.status_int)
        self._assert_credential_bypass(response)
        self.assertEqual({'reports': []}, response.json)

    @ddt.data('http_basic', 'keystone')
    def test_channel_ignores_invalid_external_credentials(self, strategy):
        app = self._make_app(strategy)
        headers = self._headers()
        headers.update({'Authorization': 'Basic not-valid-base64!',
                        'X-Auth-Token': 'not-a-keystone-token'})
        response = app.post_json(
            '/v1/agent_channel/%s/reports' % self.node.uuid, [],
            headers=headers, expect_errors=True)
        self.assertEqual(http_client.OK, response.status_int)
        self._assert_credential_bypass(response)
        self.assertEqual({'reports': []}, response.json)

    @ddt.data('http_basic', 'keystone')
    def test_public_work_still_requires_the_agent_token(self, strategy):
        response = self._post(self._make_app(strategy), 'work', token=None)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)
        self._assert_credential_bypass(response)
        self.assertIn(TOKEN_HEADER, json.loads(
            response.json['error_message'])['faultstring'])

    @ddt.data('http_basic', 'keystone')
    def test_public_reports_still_requires_the_agent_token(self, strategy):
        response = self._post(self._make_app(strategy), 'reports', token=None)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)
        self._assert_credential_bypass(response)
        self.assertIn(TOKEN_HEADER, json.loads(
            response.json['error_message'])['faultstring'])
        self.assertEqual(self.command, self.dbapi.get_agent_channel_command(
            self.command['uuid']))

    @ddt.data('http_basic', 'keystone')
    def test_public_work_rejects_the_wrong_agent_token(self, strategy):
        response = self._post(self._make_app(strategy), 'work',
                              token=WRONG_TOKEN)
        self._assert_credential_bypass(response)
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)
        self.assertEqual(self.command, self.dbapi.get_agent_channel_command(
            self.command['uuid']))

    @ddt.data('http_basic', 'keystone')
    def test_public_reports_rejects_the_wrong_agent_token(self, strategy):
        response = self._post(self._make_app(strategy), 'reports',
                              token=WRONG_TOKEN)
        self._assert_credential_bypass(response)
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)
        self.assertEqual(self.command, self.dbapi.get_agent_channel_command(
            self.command['uuid']))

    @ddt.data('http_basic', 'keystone')
    def test_adjacent_paths_still_require_external_credentials(self, strategy):
        app = self._make_app(strategy)
        prefix = '/v1/agent_channel/%s' % self.node.uuid
        paths = ['/v1/nodes', '/v1/agent_channel', prefix,
                 prefix + '/commands', prefix + '/workshop',
                 prefix + '/reports_extra', prefix + '/work/extra',
                 prefix + '/reports/extra']
        for path in paths:
            with self.subTest(path=path):
                response = app.post_json(
                    path, {}, headers={api_base.Version.string: '1.101',
                                       TOKEN_HEADER: TOKEN},
                    expect_errors=True)
                self.assertEqual(http_client.UNAUTHORIZED, response.status_int)
                if strategy == 'keystone':
                    self.assertEqual('keystone', response.headers[
                        'X-Test-Credential-Middleware'])
                else:
                    self.assertIn('WWW-Authenticate', response.headers)
        if strategy == 'keystone':
            self.assertEqual(paths, self.credential_requests)

    def _assert_named_policy(self, action, sibling):
        app = self._make_app('keystone')
        # Basic/noauth retain their existing policy semantics. Keystone uses
        # the named oslo.policy rules even when credentials bypass middleware.
        policy.get_enforcer().set_rules(oslo_policy.Rules.from_dict({
            'baremetal:node:agent_channel:%s' % action: '!',
            'baremetal:node:agent_channel:%s' % sibling: '@'}), overwrite=True)
        denied = self._post(app, action)
        self.assertEqual(http_client.FORBIDDEN, denied.status_int)
        self._assert_credential_bypass(denied)
        self.assertEqual(self.command, self.dbapi.get_agent_channel_command(
            self.command['uuid']))
        allowed = self._post(app, sibling)
        self.assertEqual(http_client.OK, allowed.status_int)
        self._assert_credential_bypass(allowed)

    def test_public_work_keeps_its_named_policy_rule(self):
        self._assert_named_policy('work', 'reports')

    def test_public_reports_keeps_its_named_policy_rule(self):
        self._assert_named_policy('reports', 'work')
