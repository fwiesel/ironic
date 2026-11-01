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

Both routes are served from the current API version (1.101) and introduce
no new microversion. They require the exact
node UUID in the URL, and authenticate the ``X-Ironic-Agent-Token`` header
against the session token verifiers in the journal. They do not bypass the
standard policy layer. The agent token never appears in the responses and
no verifier ever leaves the journal.
"""

import hashlib
from http import client as http_client

import ddt
from oslo_utils import uuidutils
import sqlalchemy

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

V115 = '1.101'  # the branch's pinned maximum microversion
V114 = '1.100'  # the microversion just below the gate

NON_STRING_COMMAND_IDS = {
    'dict': {'private-command-id': 'must-not-echo-command-id'},
    'list': ['must-not-echo-command-id'],
    'null': None,
    'integer': 123,
    'float': 1.5,
    'true': True,
    'false': False,
}


@ddt.ddt
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

    def _report_entry(self, command, kind='result', epoch=None, payload=None):
        return {'command_id': command['uuid'],
                'workflow_epoch': (command['workflow_epoch']
                                   if epoch is None else epoch),
                'kind': kind,
                'payload': {'ok': True} if payload is None else payload}

    def _assert_invalid_command_id_response(self, response):
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)
        self.assertNotRegex(
            response.text.lower(),
            r'\b(sql|select|insert|update|delete|sqlalchemy|sqlite3|'
            r'programmingerror|operationalerror)\b|\[parameters:')
        for secret in (TOKEN, VERIFIER, 'must-not-echo-'):
            self.assertNotIn(secret, response.text)

    def _successor(self, epoch=EPOCH + 1, verifier=VERIFIER):
        return self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), epoch,
            self.conductor.id, OWNER_INCARNATION, verifier, PROFILE)

    def _publish_for(self, session, invocation_key):
        return self.dbapi.publish_agent_command(
            session['uuid'], session['workflow_epoch'], invocation_key, NAME,
            {'mode': 'iscsi'}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, session['fence'])

    def _assert_disabled(self, action, body, headers=None):
        self.config(enable_ramdisk_endpoints=False, group='api')
        journal_queries = []

        def record_query(conn, cursor, statement, parameters, context,
                         executemany):
            if 'agent_channel_' in statement.lower():
                journal_queries.append(statement)

        engine = sqlalchemy.engine.Engine
        sqlalchemy.event.listen(engine, 'before_cursor_execute', record_query)
        try:
            response = self.post_json(
                '/agent_channel/%s/%s' % (self.node.uuid, action),
                params=body, headers=headers or self._headers(),
                expect_errors=True)
        finally:
            sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                    record_query)
        self.assertEqual(http_client.FORBIDDEN, response.status_int)
        self.assertEqual([], journal_queries,
                         'disabled routes accessed the channel journal')

    # -- ramdisk endpoint switch --------------------------------------------

    def test_disabled_work_rejects_before_journal_access(self):
        command = self._publish('disabled-work')
        self._assert_disabled('work', self._work_body())
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))

    def test_disabled_reports_rejects_before_journal_access(self):
        command = self._publish('disabled-reports')
        self._assert_disabled('reports', [self._report_entry(command)])
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))

    def test_disabled_work_rejects_without_token_or_valid_body(self):
        self._assert_disabled('work', [],
                              headers={api_base.Version.string: V115})

    def test_disabled_reports_rejects_without_token_or_valid_body(self):
        self._assert_disabled('reports', {},
                              headers={api_base.Version.string: V115})

    # -- microversion gating ------------------------------------------------

    def test_work_route_is_hidden_before_115(self):
        response = self._post_work(
            self._work_body(), headers=self._headers(version=V114))
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    def test_reports_route_is_hidden_before_115(self):
        response = self._post_reports(
            [], headers=self._headers(version=V114))
        self.assertEqual(http_client.NOT_FOUND, response.status_int)

    def test_work_route_answers_the_pinned_product_header(self):
        # Clients can send the standard header under the product name
        # ("OpenStack-API-Version: ironic 1.101"). That form must
        # negotiate 1.101 and reach the route, not be silently treated
        # as the minimum API version.
        headers = {'OpenStack-API-Version': 'ironic %s' % V115,
                   TOKEN_HEADER: TOKEN}
        response = self._post_work(self._work_body(), headers=headers)
        self.assertEqual(http_client.OK, response.status_int)

    def test_work_unauthenticated_post_answers_at_the_product_header(self):
        # An unauthenticated client posts without a token. At the
        # product-name header the route must answer with a defined
        # error (missing token), never 404.
        headers = {'OpenStack-API-Version': 'ironic %s' % V115}
        response = self._post_work(self._work_body(), headers=headers)
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

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

    @ddt.data(*NON_STRING_COMMAND_IDS)
    def test_reports_reject_non_string_command_ids_without_storing(self, case):
        command = self._publish('invalid-id-%s' % case)
        entry = self._report_entry(
            command, payload={'private': 'must-not-echo-report-payload'})
        entry['command_id'] = NON_STRING_COMMAND_IDS[case]
        response = self._post_reports([entry])
        self._assert_invalid_command_id_response(response)
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))

    @ddt.data(*NON_STRING_COMMAND_IDS)
    def test_reports_invalid_later_command_id_rolls_back_whole_batch(
            self, case):
        oldest = self._publish('invalid-id-old-%s' % case)
        successor = self._successor()
        newest = self._publish_for(successor, 'invalid-id-new-%s' % case)
        invalid = self._report_entry(
            newest, payload={'private': 'must-not-echo-invalid-payload'})
        invalid['command_id'] = NON_STRING_COMMAND_IDS[case]
        response = self._post_reports([
            self._report_entry(
                oldest, payload={'private': 'must-not-echo-valid-payload'}),
            invalid])
        self._assert_invalid_command_id_response(response)
        for command in (oldest, newest):
            self.assertEqual(command, self.dbapi.get_agent_channel_command(
                command['uuid']))

    @ddt.data('', 'not-a-uuid')
    def test_reports_unknown_string_command_id_is_a_negative_ack(
            self, command_id):
        command = self._publish('unknown-string')
        entry = self._report_entry(command)
        entry['command_id'] = command_id
        response = self._post_reports([entry])
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual(
            {'reports': [{'command_id': command_id, 'kind': 'result',
                          'disposition': 'unknown_command'}]}, response.json)
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))

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

    # -- pregenerated tokens shared across executions ------------------------

    def test_shared_token_reports_current_command(self):
        successor = self._successor()
        command = self._publish_for(successor, 'current')
        response = self._post_reports([self._report_entry(command)])
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual([{'command_id': command['uuid'], 'kind': 'result',
                           'disposition': 'acknowledged'}],
                         response.json['reports'])
        self.assertEqual({'ok': True}, self.dbapi.get_agent_channel_command(
            command['uuid'])['result'])

    def test_shared_token_reports_historical_command_and_duplicate(self):
        middle = self._successor()
        command = self._publish_for(middle, 'historical')
        self._successor(epoch=EPOCH + 2)
        entry = self._report_entry(command)
        first = self._post_reports([entry])
        self.assertEqual(http_client.OK, first.status_int)
        self.assertEqual('acknowledged',
                         first.json['reports'][0]['disposition'])
        second = self._post_reports([entry])
        self.assertEqual(http_client.OK, second.status_int)
        self.assertEqual('duplicate',
                         second.json['reports'][0]['disposition'])
        self.assertEqual({'ok': True}, self.dbapi.get_agent_channel_command(
            command['uuid'])['result'])

    def test_shared_token_same_epoch_reports_bind_each_command_uuid(self):
        oldest = self._publish('same-epoch-old')
        successor = self._successor(epoch=EPOCH)
        newest = self._publish_for(successor, 'same-epoch-new')
        entries = [self._report_entry(newest, payload={'execution': 'new'}),
                   self._report_entry(oldest, payload={'execution': 'old'})]
        response = self._post_reports(entries)
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual(
            [{'command_id': entry['command_id'], 'kind': 'result',
              'disposition': 'acknowledged'} for entry in entries],
            response.json['reports'])
        duplicate = self._post_reports(entries)
        self.assertEqual(http_client.OK, duplicate.status_int)
        self.assertEqual(
            [{'command_id': entry['command_id'], 'kind': 'result',
              'disposition': 'duplicate'} for entry in entries],
            duplicate.json['reports'])
        for entry in entries:
            stored = self.dbapi.get_agent_channel_command(entry['command_id'])
            self.assertEqual(entry['payload'], stored['result'])
        self.assertEqual(states.AVAILABLE,
                         self.dbapi.get_node_by_uuid(
                             self.node.uuid).provision_state)

    def test_shared_token_mixed_batch_binds_each_stored_command_epoch(self):
        oldest = self._publish('oldest')
        middle = self._successor()
        historical = self._publish_for(middle, 'middle')
        current = self._successor(epoch=EPOCH + 2)
        newest = self._publish_for(current, 'newest')
        commands = [newest, oldest, historical]
        entries = [self._report_entry(command, kind='receipt')
                   for command in commands]
        entries.append(entries[0].copy())
        response = self._post_reports(entries)
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual(
            [{'command_id': entry['command_id'], 'kind': 'receipt',
              'disposition': ('duplicate' if index == 3 else 'acknowledged')}
             for index, entry in enumerate(entries)],
            response.json['reports'])
        for command in commands:
            stored = self.dbapi.get_agent_channel_command(command['uuid'])
            self.assertEqual({'ok': True}, stored['receipt'])
            self.assertIsNone(stored['result'])
        self.assertEqual(states.AVAILABLE,
                         self.dbapi.get_node_by_uuid(
                             self.node.uuid).provision_state)

    def test_shared_token_wrong_epoch_does_not_store_a_report(self):
        successor = self._successor()
        command = self._publish_for(successor, 'wrong-epoch')
        response = self._post_reports([
            self._report_entry(command, epoch=EPOCH)])
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual('wrong_epoch',
                         response.json['reports'][0]['disposition'])
        self.assertIsNone(self.dbapi.get_agent_channel_command(
            command['uuid'])['result'])

    def test_distinct_tokens_cannot_report_each_others_commands(self):
        old_command = self._publish('old')
        successor = self._successor(verifier=OTHER_VERIFIER)
        new_command = self._publish_for(successor, 'new')
        for token, command in ((TOKEN, new_command),
                               (OTHER_TOKEN, old_command)):
            response = self._post_reports(
                [self._report_entry(command)],
                headers=self._headers(token=token))
            self.assertEqual(http_client.OK, response.status_int)
            self.assertEqual('unknown_command',
                             response.json['reports'][0]['disposition'])
            self.assertIsNone(self.dbapi.get_agent_channel_command(
                command['uuid'])['result'])

    def test_shared_token_cannot_report_a_command_of_another_node(self):
        other_node = obj_utils.create_test_node(
            self.context, uuid=uuidutils.generate_uuid())
        other_session = self._activate(other_node.uuid,
                                       uuidutils.generate_uuid())
        command = self._publish_for(other_session, 'other-node')
        response = self._post_reports([self._report_entry(command)])
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual('unknown_command',
                         response.json['reports'][0]['disposition'])
        self.assertIsNone(self.dbapi.get_agent_channel_command(
            command['uuid'])['result'])

    def test_wrong_token_cannot_store_a_report(self):
        command = self._publish('wrong-token')
        response = self._post_reports(
            [self._report_entry(command)],
            headers=self._headers(token=OTHER_TOKEN))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)
        self.assertIsNone(self.dbapi.get_agent_channel_command(
            command['uuid'])['result'])

    def test_empty_reports_batch_authenticates_shared_token(self):
        self._successor()
        response = self._post_reports([])
        self.assertEqual(http_client.OK, response.status_int)
        self.assertEqual({'reports': []}, response.json)
        response = self._post_reports(
            [], headers=self._headers(token=OTHER_TOKEN))
        self.assertEqual(http_client.UNAUTHORIZED, response.status_int)
        response = self._post_reports(
            [], headers={api_base.Version.string: V115})
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)

    def test_malformed_later_entry_rolls_back_earlier_report(self):
        command = self._publish('rollback')
        response = self._post_reports([self._report_entry(command), None])
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))

    def test_shared_token_malformed_later_payload_rolls_back_whole_batch(self):
        oldest = self._publish('rollback-old')
        successor = self._successor()
        newest = self._publish_for(successor, 'rollback-new')
        response = self._post_reports([
            self._report_entry(oldest),
            self._report_entry(newest, payload=['not-an-object'])])
        self.assertEqual(http_client.BAD_REQUEST, response.status_int)
        for command in (oldest, newest):
            self.assertEqual(command, self.dbapi.get_agent_channel_command(
                command['uuid']))

    def test_shared_token_work_keeps_explicit_identity_and_epoch(self):
        self._publish('old-work')
        successor = self._successor()
        command = self._publish_for(successor, 'new-work')
        old = self._post_work(self._work_body())
        self.assertEqual(http_client.OK, old.status_int)
        self.assertEqual('draining', old.json['state'])
        self.assertEqual('no_work', old.json['outcome'])
        self.assertIsNone(old.json['command'])
        body = self._work_body(session_uuid=successor['uuid'],
                               execution_id=successor['execution_id'],
                               epoch=successor['workflow_epoch'])
        current = self._post_work(body)
        self.assertEqual(http_client.OK, current.status_int)
        self.assertEqual(command['uuid'], current.json['command']['uuid'])
        body['execution'] = self.execution_id
        mismatch = self._post_work(body)
        self.assertEqual(http_client.UNAUTHORIZED, mismatch.status_int)
        body['execution'] = successor['execution_id']
        body['workflow_epoch'] = EPOCH
        stale = self._post_work(body)
        self.assertEqual(http_client.OK, stale.status_int)
        self.assertEqual('no_work', stale.json['outcome'])
        self.assertEqual(successor['workflow_epoch'], stale.json['epoch'])
        self.assertIsNone(stale.json['command'])
        self.assertEqual(command, self.dbapi.get_agent_channel_command(
            command['uuid']))
