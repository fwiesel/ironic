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

"""API tests for the agent channel work and reports endpoints.

The frozen contract adds two public routes for the deploy ramdisk:

* ``POST /v1/agent_channel/{node_uuid}/work`` — the agent polls for one
  bounded piece of work. The request body carries the session UUID, the
  execution UUID, the poller's believed workflow epoch, an opaque poll ID,
  and a wait budget. The response carries the session execution epoch, its
  state, one outcome (``command``, ``no_work``, ``not_ready``, ``closed``),
  an optional command envelope, and a ``retry_after`` hint. The server
  clamps every wait budget to zero until measured capacity proves long
  waits are safe.
* ``POST /v1/agent_channel/{node_uuid}/reports`` — the agent submits an
  independent bounded list of report entries. Each entry has a command ID,
  a workflow epoch, a kind (``receipt``, ``acceptance``, ``result``), and a
  payload. The response commits one acknowledgment per entry with its
  disposition.

Both routes are served from the current API version (1.115) and introduce
no new microversion. They require the exact
node UUID in the URL, and authenticate the ``X-Ironic-Agent-Token`` header
against the session token verifiers in the journal. They do not bypass the
standard policy layer. The agent token never appears in the responses and
no verifier ever leaves the journal.
"""

import hashlib
from http import client as http_client

from oslo_utils import uuidutils

from ironic.api.controllers import base as api_base
from ironic.common import states
from ironic.tests.unit.api import base as test_api_base
from ironic.tests.unit.db import utils as db_utils
from ironic.tests.unit.objects import utils as obj_utils

TOKEN_HEADER = 'X-Ironic-Agent-Token'
TOKEN = 'ramdisk-token-artichoke'
VERIFIER = hashlib.sha256(TOKEN.encode('utf-8')).hexdigest()
OTHER_TOKEN = 'ramdisk-token-endive'
OTHER_VERIFIER = hashlib.sha256(OTHER_TOKEN.encode('utf-8')).hexdigest()
OWNER_INCARNATION = 'incarnation-1'
PROFILE = 'agent-poll'
EPOCH = 1
NAME = 'power_on'
BOUNDARY = 'completion'

V115 = '1.115'
V114 = '1.114'


class AgentChannelApiTestCase(test_api_base.BaseApiTest):

    def setUp(self):
        super(AgentChannelApiTestCase, self).setUp()
        self.node = obj_utils.create_test_node(self.context)
        self.conductor = db_utils.create_test_conductor()
        self.execution_id = uuidutils.generate_uuid()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, self.execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, VERIFIER, PROFILE)
        self.session_uuid = self.session['uuid']
        self.fence = self.session['fence']

    def _headers(self, version=V115, token=TOKEN, extra=None):
        headers = {
            api_base.Version.string: version,
            TOKEN_HEADER: token,
        }
        if extra:
            headers.update(extra)
        return headers

    def _activate(self, node_uuid, execution_id, verifier=VERIFIER):
        return self.dbapi.activate_agent_channel(
            node_uuid, execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, verifier, PROFILE)

    def _publish(self, invocation_key, session_uuid=None, fence=None):
        return self.dbapi.publish_agent_command(
            session_uuid or self.session_uuid, EPOCH, invocation_key, NAME,
            {'mode': 'iscsi'}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION,
            self.fence if fence is None else fence)

    def _work_body(self, session_uuid=None, execution_id=None,
                   epoch=EPOCH, **extra):
        body = {
            'session': session_uuid or self.session_uuid,
            'execution': execution_id or self.execution_id,
            'workflow_epoch': epoch,
            'poll_id': 'poll-1',
            'wait_seconds': 0,
        }
        body.update(extra)
        return body

    def _post_work(self, body, headers=None, expect_errors=True):
        return self.post_json(
            '/agent_channel/%s/work' % self.node.uuid,
            params=body,
            headers=headers or self._headers(),
            expect_errors=expect_errors)

    def _post_reports(self, entries, headers=None, expect_errors=True):
        return self.post_json(
            '/agent_channel/%s/reports' % self.node.uuid,
            params=entries,
            headers=headers or self._headers(),
            expect_errors=expect_errors)

    # -- microversion gating ------------------------------------------------

    def test_work_route_is_hidden_before_115(self):
        response = self._post_work(
            self._work_body(), headers=self._headers(version=V114))
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    def test_reports_route_is_hidden_before_115(self):
        response = self._post_reports(
            [], headers=self._headers(version=V114))
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    # -- work endpoint auth -------------------------------------------------

    def test_work_requires_the_token_header(self):
        headers = self._headers()
        headers.pop(TOKEN_HEADER)
        response = self._post_work(self._work_body(), headers=headers)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    def test_work_rejects_a_wrong_token(self):
        response = self._post_work(
            self._work_body(), headers=self._headers(token=OTHER_TOKEN))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)

    def test_work_rejects_an_unknown_node(self):
        response = self.post_json(
            '/agent_channel/%s/work' % uuidutils.generate_uuid(),
            params=self._work_body(), headers=self._headers(),
            expect_errors=True)
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    def test_work_rejects_a_malformed_node_uuid(self):
        response = self.post_json(
            '/agent_channel/not-a-uuid/work',
            params=self._work_body(), headers=self._headers(),
            expect_errors=True)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    def test_work_rejects_a_wrong_execution_in_the_body(self):
        response = self._post_work(
            self._work_body(execution_id=uuidutils.generate_uuid()))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)

    def test_work_rejects_an_unknown_session_in_the_body(self):
        response = self._post_work(
            self._work_body(session_uuid=uuidutils.generate_uuid()))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)

    def test_work_requires_a_json_object_body(self):
        response = self._post_work([])
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    # -- work endpoint behavior ---------------------------------------------

    def test_work_delivers_a_command_envelope(self):
        command = self._publish('step-1')
        response = self._post_work(self._work_body())
        self.assertEqual(http_client.OK, response.status_int)
        body = response.json
        self.assertEqual('command', body['outcome'])
        self.assertEqual('active', body['state'])
        self.assertEqual(EPOCH, body['epoch'])
        self.assertEqual(0, body['retry_after'])
        envelope = body['command']
        self.assertEqual(command['uuid'], envelope['uuid'])
        self.assertEqual(self.session_uuid, envelope['session_uuid'])
        self.assertEqual(self.execution_id, envelope['execution_id'])
        self.assertEqual(EPOCH, envelope['workflow_epoch'])
        self.assertEqual(command['sequence'], envelope['sequence'])
        self.assertEqual(NAME, envelope['name'])
        self.assertEqual({'mode': 'iscsi'}, envelope['params'])
        self.assertEqual(BOUNDARY, envelope['reply_boundary'])
        self.assertEqual('queued', envelope['state'])
        self.assertIsNone(envelope['receipt'])

    def test_work_returns_no_work_for_a_current_session_without_commands(self):
        response = self._post_work(self._work_body())
        self.assertEqual(http_client.OK, response.status_int)
        body = response.json
        self.assertEqual('no_work', body['outcome'])
        self.assertEqual('active', body['state'])
        self.assertEqual(EPOCH, body['epoch'])
        self.assertIsNone(body['command'])

    def test_work_stale_epoch_returns_no_work_with_the_current_epoch(self):
        self._publish('step-1')
        response = self._post_work(self._work_body(epoch=EPOCH - 1))
        self.assertEqual(http_client.OK, response.status_int)
        body = response.json
        self.assertEqual('no_work', body['outcome'])
        self.assertEqual(EPOCH, body['epoch'],
                         'a stale poll did not report the current epoch')
        self.assertIsNone(body['command'])

    def test_work_redelivers_until_a_durable_receipt(self):
        command = self._publish('step-1')
        first = self._post_work(self._work_body()).json
        second = self._post_work(self._work_body()).json
        self.assertEqual(command['uuid'], first['command']['uuid'])
        self.assertEqual(command['uuid'], second['command']['uuid'])
        # A durable receipt stops the redelivery of that command.
        acks = self.dbapi.report_agent_channel(
            self.session_uuid,
            [{'command_id': command['uuid'],
              'workflow_epoch': EPOCH,
              'kind': 'receipt',
              'payload': {'received': True}}])
        self.assertEqual('acknowledged', acks[0]['disposition'])
        third = self._post_work(self._work_body()).json
        self.assertEqual('no_work', third['outcome'])
        self.assertIsNone(third['command'])

    def test_work_does_not_consume_or_advance(self):
        command = self._publish('step-1')
        response = self._post_work(self._work_body())
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual('command', response.json['outcome'])
        # The poll must leave the command queued.
        stored = self.dbapi.get_agent_channel_command(command['uuid'])
        self.assertEqual('queued', stored['state'],
                         'a work poll consumed the command')
        # The poll must not touch the Node row.
        node = self.dbapi.get_node_by_uuid(self.node.uuid)
        self.assertEqual(states.AVAILABLE, node.provision_state)

    # -- reports endpoint auth ----------------------------------------------

    def test_reports_requires_the_token_header(self):
        headers = self._headers()
        headers.pop(TOKEN_HEADER)
        response = self._post_reports([], headers=headers)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    def test_reports_rejects_a_wrong_token(self):
        response = self._post_reports(
            [], headers=self._headers(token=OTHER_TOKEN))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)

    def test_reports_rejects_an_unknown_node(self):
        response = self.post_json(
            '/agent_channel/%s/reports' % uuidutils.generate_uuid(),
            params=[], headers=self._headers(), expect_errors=True)
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    def test_reports_requires_a_json_list_body(self):
        response = self._post_reports({'not': 'a list'})
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    # -- reports endpoint behavior ------------------------------------------

    def test_reports_commits_acknowledgments(self):
        command = self._publish('step-1')
        response = self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'receipt',
             'payload': {'received': True}},
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'result',
             'payload': {'ok': True}},
        ])
        self.assertEqual(http_client.OK, response.status_int)
        acks = response.json['reports']
        self.assertEqual(2, len(acks))
        self.assertEqual(command['uuid'], acks[0]['command_id'])
        self.assertEqual('receipt', acks[0]['kind'])
        self.assertEqual('acknowledged', acks[0]['disposition'])
        self.assertEqual('result', acks[1]['kind'])
        self.assertEqual('acknowledged', acks[1]['disposition'])

    def test_reports_acks_do_not_carry_the_payload(self):
        command = self._publish('step-1')
        response = self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'receipt',
             'payload': {'secret': 'must-not-return'}},
        ])
        ack = response.json['reports'][0]
        self.assertNotIn('payload', ack)
        self.assertNotIn('secret', ack)

    def test_reports_duplicate_entry_is_acknowledged_as_duplicate(self):
        command = self._publish('step-1')
        entry = {'command_id': command['uuid'],
                 'workflow_epoch': EPOCH,
                 'kind': 'acceptance',
                 'payload': {'admitted': True}}
        first = self._post_reports([entry]).json['reports'][0]
        self.assertEqual('acknowledged', first['disposition'])
        second = self._post_reports([entry]).json['reports'][0]
        self.assertEqual('duplicate', second['disposition'])

    def test_reports_unknown_command_gets_a_negative_disposition(self):
        response = self._post_reports([
            {'command_id': uuidutils.generate_uuid(),
             'workflow_epoch': EPOCH,
             'kind': 'receipt',
             'payload': {'received': True}},
        ])
        ack = response.json['reports'][0]
        self.assertEqual('unknown_command', ack['disposition'])

    def test_reports_wrong_epoch_gets_a_negative_disposition(self):
        command = self._publish('step-1')
        response = self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH + 1,
             'kind': 'receipt',
             'payload': {'received': True}},
        ])
        ack = response.json['reports'][0]
        self.assertEqual('wrong_epoch', ack['disposition'])

    def test_reports_unsupported_kind_gets_a_negative_disposition(self):
        command = self._publish('step-1')
        response = self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'terminal_closed',
             'payload': {'power': 'off'}},
        ])
        ack = response.json['reports'][0]
        self.assertEqual('unsupported_kind', ack['disposition'])

    def test_reports_from_a_superseded_session_stay_acknowledged(self):
        command = self._publish('step-1')
        successor = self._activate(self.node.uuid,
                                   uuidutils.generate_uuid(),
                                   verifier=OTHER_VERIFIER)
        # The old session reads draining; its token still authenticates its
        # own reports, which the journal acknowledges for the stored command.
        old_headers = self._headers(token=TOKEN)
        response = self._post_reports(
            [{'command_id': command['uuid'],
              'workflow_epoch': EPOCH,
              'kind': 'result',
              'payload': {'ok': True}}],
            headers=old_headers)
        self.assertEqual(http_client.OK, response.status_int)
        ack = response.json['reports'][0]
        self.assertEqual('acknowledged', ack['disposition'])
        # The successor token authenticates a separate session.
        response = self._post_work(
            self._work_body(session_uuid=successor['uuid'],
                            execution_id=successor['execution_id']),
            headers=self._headers(token=OTHER_TOKEN))
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual('no_work', response.json['outcome'])

    def test_reports_response_never_leaks_the_token(self):
        command = self._publish('step-1')
        response = self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'receipt',
             'payload': {'received': True}},
        ])
        body = response.json
        self.assertNotIn(TOKEN, str(body))
        self.assertNotIn('token', str(body.get('reports', [])))

    def test_agent_channel_not_in_version_index_before_115(self):
        data = self.get_json(
            '/v1', path_prefix='', headers={api_base.Version.string: V114})
        self.assertNotIn('agent_channel', data)

    def test_agent_channel_in_version_index_at_115(self):
        data = self.get_json(
            '/v1', path_prefix='', headers={api_base.Version.string: V115})
        self.assertIn('agent_channel', data)

    def test_reports_do_not_change_the_node_state(self):
        command = self._publish('step-1')
        self._post_reports([
            {'command_id': command['uuid'],
             'workflow_epoch': EPOCH,
             'kind': 'result',
             'payload': {'ok': True}},
        ])
        node = self.dbapi.get_node_by_uuid(self.node.uuid)
        self.assertEqual(states.AVAILABLE, node.provision_state,
                         'the reports path changed the node state')
