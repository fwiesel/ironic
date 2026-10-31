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

"""Unit tests for the agent channel journal work rendezvous.

The report half of the rendezvous contract pins reply ingestion. These tests
pin the work half: the journal resolves one agent work poll against the node
head and the session fence, and returns the session execution epoch, its
state, and one outcome. A work poll is the delivery side of the wire ``work``
request; it never consumes a command, never advances the workflow, and never
touches the Node row.

DB API contract under test, all on ``dbapi.get_instance()``:

* ``poll_agent_channel_work(node_uuid, session_uuid, execution_id,
  workflow_epoch)`` returns a dict with the keys ``outcome``, ``state``,
  ``epoch``, ``command``, and ``retry_after``. Extra keys are allowed.

Outcomes under test:

* ``command``: the current active session has a queued command that was not
  yet accepted durably; the ``command`` key carries the envelope. The
  envelope keys are ``uuid``, ``session_uuid``, ``execution_id``,
  ``workflow_epoch``, ``sequence``, ``name``, ``params``, ``reply_boundary``,
  ``state``, ``receipt``, ``acceptance``, and ``result``. A second poll that
  has not yet reported a durable receipt returns the same command again, so
  a lost delivery response stays retrievable.
* ``no_work``: the channel is ready and the session may poll, but there is
  nothing to deliver. A superseded session reads state ``draining`` and
  gets ``no_work``. A poll with a stale workflow epoch gets ``no_work`` and
  the current epoch so the agent can resynchronize.
* ``not_ready``: the channel is not ready for this poller. An unknown
  session, a session of another node, a session whose execution does not
  match, and a node that was never activated are all ``not_ready``.
* ``closed``: the session is closed and carries no new work.

Fencing rules under test:

* A poll makes no Node update, no workflow-epoch advance, no mutation of the
  head or the session, and no change to any command state. The command stays
  ``queued`` until a later consumption unit under the task fence.
* A stale session, session epoch, or execution identity receives no work and
  no consumption.
* An unknown node raises ``exception.NodeNotFound``; the endpoint path names
  the node, so an unknown node is a 404, not a channel state.
"""

import uuid as uuid_lib

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
POLL_KEYS = ('outcome', 'state', 'epoch', 'command', 'retry_after')
ENVELOPE_KEYS = ('uuid', 'session_uuid', 'execution_id', 'workflow_epoch',
                 'sequence', 'name', 'params', 'reply_boundary', 'state',
                 'receipt', 'acceptance', 'result')


class AgentChannelWorkTestCase(base.DbTestCase):

    def setUp(self):
        super(AgentChannelWorkTestCase, self).setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()
        self.execution_id = uuidutils.generate_uuid()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, self.execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        self.session_uuid = self.session['uuid']
        self.fence = self.session['fence']

    def _activate(self, node_uuid, execution_id):
        return self.dbapi.activate_agent_channel(
            node_uuid, execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)

    def _publish(self, invocation_key, params=None, session_uuid=None,
                 fence=None, epoch=EPOCH):
        return self.dbapi.publish_agent_command(
            session_uuid or self.session_uuid, epoch, invocation_key, NAME,
            {} if params is None else params, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION,
            self.fence if fence is None else fence)

    def _poll(self, node_uuid=None, session_uuid=None, execution_id=None,
              epoch=EPOCH):
        poll = getattr(self.dbapi, 'poll_agent_channel_work', None)
        self.assertTrue(
            callable(poll),
            'dbapi.poll_agent_channel_work is missing or not callable; the '
            'agent channel work rendezvous API is not implemented yet')
        return poll(node_uuid or self.node.uuid,
                    session_uuid or self.session_uuid,
                    execution_id or self.execution_id, epoch)

    def _require_keys(self, result, keys, context):
        for key in keys:
            self.assertIn(key, result, '%s result lacks key %s'
                          % (context, key))

    def _report_receipt(self, command_uuid, session_uuid=None):
        self.dbapi.report_agent_channel(
            session_uuid or self.session_uuid,
            [{'command_id': command_uuid,
              'workflow_epoch': EPOCH,
              'kind': 'receipt',
              'payload': {'received': True}}])

    def _session_state(self, session_uuid):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(models.AgentChannelSession.state).where(
                models.AgentChannelSession.uuid == session_uuid)
            return conn.execute(stmt).scalar_one()

    def _set_session_state(self, session_uuid, state):
        engine = enginefacade.writer.get_engine()
        with engine.begin() as conn:
            conn.execute(
                sqlalchemy.update(models.AgentChannelSession)
                .where(models.AgentChannelSession.uuid == session_uuid)
                .values(state=state))

    def test_poll_delivers_the_oldest_queued_command_envelope(self):
        params = {'mode': 'iscsi', 'target': {'ip': '192.0.2.1'}}
        command = self._publish('step-1', params=params)
        result = self._poll()
        self._require_keys(result, POLL_KEYS, 'poll')
        self.assertEqual('command', result['outcome'])
        self.assertEqual('active', result['state'])
        self.assertEqual(EPOCH, result['epoch'])
        self.assertIsInstance(result['retry_after'], int)
        envelope = result['command']
        self._require_keys(envelope, ENVELOPE_KEYS, 'envelope')
        self.assertEqual(command['uuid'], envelope['uuid'])
        self.assertEqual(self.session_uuid, envelope['session_uuid'])
        self.assertEqual(self.execution_id, envelope['execution_id'])
        self.assertEqual(EPOCH, envelope['workflow_epoch'])
        self.assertEqual(command['sequence'], envelope['sequence'])
        self.assertEqual(NAME, envelope['name'])
        self.assertEqual(params, envelope['params'])
        self.assertEqual(BOUNDARY, envelope['reply_boundary'])
        self.assertEqual('queued', envelope['state'])
        self.assertIsNone(envelope['receipt'])
        self.assertIsNone(envelope['acceptance'])
        self.assertIsNone(envelope['result'])

    def test_poll_returns_no_work_for_active_session_without_commands(self):
        result = self._poll()
        self.assertEqual('no_work', result['outcome'])
        self.assertEqual('active', result['state'])
        self.assertEqual(EPOCH, result['epoch'])
        self.assertIsNone(result['command'])

    def test_redelivery_until_durable_receipt_then_next_command(self):
        first = self._publish('step-1')
        second = self._publish('step-2')
        # A poll before a durable receipt returns the same command again, so
        # a lost delivery response stays retrievable.
        first_delivery = self._poll()['command']
        redelivery = self._poll()['command']
        self.assertEqual(first['uuid'], first_delivery['uuid'])
        self.assertEqual(first['uuid'], redelivery['uuid'])
        self.assertEqual(first['sequence'], redelivery['sequence'])
        # A durable receipt stops the redelivery of that command.
        self._report_receipt(first['uuid'])
        second_delivery = self._poll()['command']
        self.assertEqual(second['uuid'], second_delivery['uuid'])
        self.assertEqual(second['sequence'], first['sequence'] + 1)
        # Without a receipt for the second command it is delivered again.
        self.assertEqual(second['uuid'],
                         self._poll()['command']['uuid'])
        # With both receipts durable there is nothing left to deliver.
        self._report_receipt(second['uuid'])
        result = self._poll()
        self.assertEqual('no_work', result['outcome'])
        self.assertIsNone(result['command'])

    def test_poll_is_read_only_no_consume_no_advance(self):
        command = self._publish('step-1')
        head_epoch_before = self._head_epoch()
        node_before = self.dbapi.get_node_by_uuid(self.node.uuid).as_dict()
        result = self._poll()
        self.assertEqual('command', result['outcome'])
        stored = self.dbapi.get_agent_channel_command(command['uuid'])
        self.assertEqual('queued', stored['state'],
                         'a work poll consumed the command')
        self.assertEqual('queued', result['command']['state'])
        self.assertEqual(head_epoch_before, self._head_epoch(),
                         'a work poll advanced the workflow epoch')
        node_after = self.dbapi.get_node_by_uuid(self.node.uuid).as_dict()
        assert_values = {'provision_state', 'target_provision_state',
                         'power_state', 'updated_at'}
        for key in assert_values.intersection(node_before).intersection(
                node_after):
            self.assertEqual(node_before[key], node_after[key],
                             'a work poll changed node field %s' % key)

    def _head_epoch(self):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelHead.workflow_epoch).where(
                    models.AgentChannelHead.node_id == self.node.id)
            return conn.execute(stmt).scalar_one()

    def test_superseded_session_reads_draining_and_gets_no_work(self):
        command = self._publish('step-1')
        self._activate(self.node.uuid, uuidutils.generate_uuid())
        result = self._poll()
        self.assertEqual('no_work', result['outcome'])
        self.assertEqual('draining', result['state'])
        self.assertIsNone(result['command'])
        self.assertEqual('draining', self._session_state(self.session_uuid))
        # The superseded session's command was not consumed by the poll.
        self.assertEqual('queued',
                         self.dbapi.get_agent_channel_command(
                             command['uuid'])['state'])
        # The successor session has the work authority.
        successor = self._activate(self.node.uuid,
                                   uuidutils.generate_uuid())
        follower = self._poll(session_uuid=successor['uuid'],
                              execution_id=successor['execution_id'])
        self.assertEqual('no_work', follower['outcome'])
        successor_command = self.dbapi.publish_agent_command(
            successor['uuid'], EPOCH, 'step-2', NAME, {}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, successor['fence'])
        delivered = self._poll(session_uuid=successor['uuid'],
                               execution_id=successor['execution_id'])
        self.assertEqual('command', delivered['outcome'])
        self.assertEqual(successor_command['uuid'],
                         delivered['command']['uuid'])

    def test_stale_epoch_poll_gets_no_work_and_current_epoch(self):
        self._publish('step-1')
        result = self._poll(epoch=EPOCH - 1)
        self.assertEqual('no_work', result['outcome'])
        self.assertEqual('active', result['state'])
        self.assertEqual(EPOCH, result['epoch'],
                         'the stale poll did not report the current epoch')
        self.assertIsNone(result['command'])
        self.assertEqual(self._head_epoch(), EPOCH,
                         'a stale poll advanced the workflow epoch')

    def test_wrong_execution_identity_is_not_ready(self):
        result = self._poll(execution_id=uuidutils.generate_uuid())
        self.assertEqual('not_ready', result['outcome'])
        self.assertIsNone(result['state'])
        self.assertIsNone(result['epoch'])
        self.assertIsNone(result['command'])

    def test_session_of_another_node_is_not_ready(self):
        other_node = db_utils.create_test_node()
        other_session = self._activate(other_node.uuid,
                                       uuidutils.generate_uuid())
        result = self._poll(session_uuid=other_session['uuid'])
        self.assertEqual('not_ready', result['outcome'])
        self.assertIsNone(result['command'])

    def test_unknown_session_is_not_ready(self):
        result = self._poll(session_uuid=uuidutils.generate_uuid())
        self.assertEqual('not_ready', result['outcome'])
        self.assertIsNone(result['command'])
        self.assertIsNone(result['state'])

    def test_node_without_channel_is_not_ready(self):
        node = db_utils.create_test_node()
        result = self._poll(node_uuid=node.uuid,
                            session_uuid=uuidutils.generate_uuid())
        self.assertEqual('not_ready', result['outcome'])
        self.assertIsNone(result['command'])
        self.assertEqual(self._head_epoch(), EPOCH,
                         'a not-ready poll advanced the workflow epoch')

    def test_unknown_node_raises_not_found(self):
        self.assertRaises(
            exception.NotFound, self._poll,
            node_uuid=str(uuid_lib.uuid4()),
            session_uuid=uuidutils.generate_uuid())

    def test_closed_session_returns_closed(self):
        self._set_session_state(self.session_uuid, 'closed')
        result = self._poll()
        self.assertEqual('closed', result['outcome'])
        self.assertEqual('closed', result['state'])
        self.assertIsNone(result['command'])

    def test_draining_session_returns_no_work(self):
        self._set_session_state(self.session_uuid, 'draining')
        result = self._poll()
        self.assertEqual('no_work', result['outcome'])
        self.assertEqual('draining', result['state'])
        self.assertIsNone(result['command'])
