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

"""Unit tests for agent channel journal report ingestion.

The journal identity seed in ``test_agent_channel_journal`` pins command
publication. These tests pin the report side of the journal contract: the
journal accepts a bounded list of agent reports for stored commands, stores
each payload in the command's report slot, and returns one durable
acknowledgment per entry. Report ingestion neither updates the Node row nor
advances any workflow epoch. A stale-epoch report, an unknown command, a
changed body for an already committed kind, and an unsupported kind are
rejected with a named disposition, never with a leak of the payload.

DB API contract under test, all on ``dbapi.get_instance()``:

* ``report_agent_channel(session_uuid, reports)`` returns a list of
  acknowledgment dicts with the keys ``command_id``, ``kind``, and
  ``disposition``. Extra keys are allowed.

Each report entry is a dict with ``command_id``, ``workflow_epoch``,
``kind``, and ``payload``. The payload is a JSON object, the same shape the
journal requires for command parameters. The report list is bounded: a
non-list or an over-long list is rejected with ``InvalidParameterValue``
before any slot changes. The valid kinds are ``receipt``, ``acceptance``,
and ``result``. The terminal-closed kind is bound by the terminal barrier
unit and is rejected here with ``unsupported_kind``.

Dispositions under test:

* ``acknowledged``: the payload is stored durably in the command's slot
  for the kind and becomes visible through ``get_agent_channel_command``.
* ``duplicate``: the same kind with the same payload is already stored;
  the stored value is unchanged and the report is acknowledged again.
* ``conflict``: a changed payload for an already stored kind is rejected.
* ``wrong_epoch``: the entry epoch does not match the stored command.
* ``unknown_command``: no stored command with that ID in the session.
* ``unsupported_kind``: a kind outside the report slot contract.

Report ingestion is the reply path: it makes no Node update and no
workflow advance, and a late valid report for a stored command of a
superseded session is still acknowledged.
"""

import json

from oslo_db.sqlalchemy import enginefacade
from oslo_utils import uuidutils
import sqlalchemy

from ironic.common import exception
from ironic.db.sqlalchemy import models
from ironic.tests.unit.db import base
from ironic.tests.unit.db import utils as db_utils

TOKEN_VERIFIER = 'deadbeef' * 8
OWNER_INCARNATION = 'incarnation-1'
PROFILE = 'agent-poll'
EPOCH = 1
NAME = 'power_on'
BOUNDARY = 'completion'
REPORT_KINDS = ('receipt', 'acceptance', 'result')
ACK_KEYS = ('command_id', 'kind', 'disposition')


class AgentChannelReportTestCase(base.DbTestCase):

    def setUp(self):
        super(AgentChannelReportTestCase, self).setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        self.session_uuid = self.session['uuid']
        self.fence = self.session['fence']
        self.command = self._publish('step-1')
        self.command_uuid = self.command['uuid']

    def _publish(self, invocation_key, params=None, epoch=EPOCH):
        return self.dbapi.publish_agent_command(
            self.session_uuid, epoch, invocation_key, NAME,
            {} if params is None else params, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, self.fence)

    def _report(self, command_id=None, kind='receipt', payload=None,
                epoch=EPOCH, session_uuid=None):
        report_agent_channel = getattr(
            self.dbapi, 'report_agent_channel', None)
        self.assertTrue(
            callable(report_agent_channel),
            'dbapi.report_agent_channel is missing or not callable; the '
            'agent channel report API is not implemented yet')
        entry = {'command_id': command_id or self.command_uuid,
                 'workflow_epoch': epoch,
                 'kind': kind,
                 'payload': {'value': 'ok'} if payload is None else payload}
        return report_agent_channel(
            session_uuid or self.session_uuid, [entry])

    def _ack(self, acks, index=0):
        self.assertLess(index, len(acks))
        for key in ACK_KEYS:
            self.assertIn(key, acks[index],
                          'ack lacks key %s' % key)
        return acks[index]

    def _stored_slot(self, kind, command_uuid=None):
        command = self.dbapi.get_agent_channel_command(
            command_uuid or self.command_uuid)
        return command.get(kind)

    def _session_epoch(self):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelHead.workflow_epoch).where(
                    models.AgentChannelHead.node_id == self.node.id)
            return conn.execute(stmt).scalar_one()

    def test_receipt_acceptance_result_are_stored_and_acknowledged(self):
        stored_payloads = {}
        for kind in REPORT_KINDS:
            payload = {'value': kind}
            acks = self._report(kind=kind, payload=payload)
            ack = self._ack(acks)
            self.assertEqual(self.command_uuid, ack['command_id'])
            self.assertEqual(kind, ack['kind'])
            self.assertEqual('acknowledged', ack['disposition'])
            stored_payloads[kind] = payload
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        for slot in REPORT_KINDS:
            self.assertEqual(stored_payloads[slot], stored.get(slot),
                             'slot %s did not hold its own payload' % slot)

    def test_unstored_kinds_read_back_as_none(self):
        self._ack(self._report(kind='receipt', payload={'value': 'r'}))
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        self.assertEqual({'value': 'r'}, stored.get('receipt'))
        self.assertIsNone(stored.get('acceptance'),
                          'unset slot acceptance is not None')
        self.assertIsNone(stored.get('result'),
                          'unset slot result is not None')

    def test_duplicate_report_is_acknowledged_without_change(self):
        payload = {'attempt': 1}
        first = self._ack(self._report(payload=payload))
        self.assertEqual('acknowledged', first['disposition'])
        second = self._ack(self._report(payload=payload))
        self.assertEqual('duplicate', second['disposition'])
        self.assertEqual(payload, self._stored_slot('receipt'))

    def test_changed_payload_for_committed_kind_is_conflict(self):
        self._report(payload={'attempt': 1})
        acks = self._report(payload={'attempt': 2})
        ack = self._ack(acks)
        self.assertEqual('conflict', ack['disposition'])
        # The first payload stays stored, unchanged.
        self.assertEqual({'attempt': 1}, self._stored_slot('receipt'))

    def test_unknown_command_gets_unknown_command(self):
        acks = self._report(command_id=uuidutils.generate_uuid())
        self.assertEqual('unknown_command', self._ack(acks)['disposition'])

    def test_report_for_command_of_another_session_is_unknown(self):
        other_session = self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        other_command = self.dbapi.publish_agent_command(
            other_session['uuid'], EPOCH, 'step-9', NAME, {}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, other_session['fence'])
        acks = self._report(command_id=other_command['uuid'])
        self.assertEqual('unknown_command', self._ack(acks)['disposition'])
        self.assertIsNone(self._stored_slot('receipt', other_command['uuid']))

    def test_wrong_epoch_report_is_rejected(self):
        acks = self._report(epoch=EPOCH + 1)
        ack = self._ack(acks)
        self.assertEqual('wrong_epoch', ack['disposition'])
        self.assertIsNone(self._stored_slot('receipt'))

    def test_unsupported_kind_is_rejected(self):
        acks = self._report(kind='terminal_closed')
        self.assertEqual('unsupported_kind', self._ack(acks)['disposition'])
        self.assertIsNone(self._stored_slot('receipt'))

    def test_unknown_session_raises_not_found(self):
        self.assertRaises(exception.NotFound, self._report,
                          session_uuid=uuidutils.generate_uuid())

    def test_invalid_payload_raises_before_any_slot(self):
        for bad in ({'nan': float('nan')}, {'value': object()}):
            self.assertRaises(
                exception.InvalidParameterValue,
                self._report, payload=bad)
            self.assertIsNone(self._stored_slot('receipt'))

    def test_report_list_must_be_a_bounded_list(self):
        report_agent_channel = self.dbapi.report_agent_channel
        self.assertRaises(exception.InvalidParameterValue,
                          report_agent_channel, self.session_uuid, 'nope')
        self.assertRaises(exception.InvalidParameterValue,
                          report_agent_channel, self.session_uuid,
                          {'command_id': self.command_uuid})
        overlong = [{'command_id': self.command_uuid,
                     'workflow_epoch': EPOCH,
                     'kind': 'receipt',
                     'payload': {'seq': i}}
                    for i in range(201)]
        self.assertRaises(exception.InvalidParameterValue,
                          report_agent_channel, self.session_uuid, overlong)
        self.assertIsNone(self._stored_slot('receipt'))

    def test_report_ingestion_makes_no_node_update_and_no_advance(self):
        before = self.dbapi.get_node_by_uuid(self.node.uuid).as_dict()
        epoch_before = self._session_epoch()
        for kind in REPORT_KINDS:
            self._ack(self._report(kind=kind))
        after = self.dbapi.get_node_by_uuid(self.node.uuid).as_dict()
        epoch_after = self._session_epoch()
        assert_values = {'provision_state', 'target_provision_state',
                         'power_state', 'updated_at'}
        for key in assert_values.intersection(before).intersection(after):
            self.assertEqual(before[key], after[key],
                             'report ingestion changed node field %s' % key)
        self.assertEqual(epoch_before, epoch_after,
                         'report ingestion advanced the workflow epoch')

    def test_late_report_for_superseded_session_is_acknowledged(self):
        # A new execution supersedes the first session; its stored command
        # keeps its journal rows and a late result stays acknowledgeable.
        self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        acks = self._report(kind='result', payload={'late': True})
        self.assertEqual('acknowledged', self._ack(acks)['disposition'])
        self.assertEqual({'late': True}, self._stored_slot('result'))

    def test_report_survives_storage_round_trip(self):
        payload = {'stdout': 'done', 'rc': 0}
        self._ack(self._report(kind='result', payload=payload))
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        self.assertEqual(payload, stored.get('result'))
        # The stored slot holds a JSON round-trip of the payload.
        self.assertEqual(json.loads(json.dumps(payload)),
                         stored.get('result'))
