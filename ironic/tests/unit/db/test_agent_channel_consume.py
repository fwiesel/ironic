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

"""Unit tests for agent channel reply consumption and reservation-safe wake.

The rendezvous unit established the delivery and report halves of the
journal contract. These tests pin the consumption half (validation-order
unit 4, "exclusive-reservation wake and backend boundaries"): a committed
reply wakes a waiting caller while that caller holds an exclusive Node
reservation, the consume path marks the journal consumed atomically under
the current fence, and the work wait holds no database transaction, no
Node-row lock, and no TaskManager reservation of its own.

DB API contracts under test, all on ``dbapi.get_instance()``:

* ``consume_agent_channel_command(node_uuid, session_uuid, command_uuid,
  workflow_epoch, fence)`` returns a dict with ``outcome`` ``reply`` and the
  consumed command under ``command``. The command dict carries the stored
  report slots and ``state`` ``consumed``.
* ``wait_agent_channel_reply(node_uuid, session_uuid, command_uuid,
  workflow_epoch, fence, timeout, wake_hint=None, wait_interval=None)``
  returns a dict with ``outcome`` ``reply`` and the consumed command, or
  ``outcome`` ``no_reply`` with no command when no reply is committed before
  the monotonic deadline or when the session is superseded while waiting.

Properties pinned here:

* A command that has no committed result cannot be consumed and stays
  ``queued``.
* Consumption happens under the same fence as publication: a stale session,
  fence, or epoch receives no consume and no duplicate advance.
* Consumption is idempotent: a repeated consume returns the stored reply and
  never advances anything a second time.
* The consume path and the wait make no Node update and no workflow-epoch
  advance, and neither requires the Node-row lock: both succeed while the
  node holds an exclusive reservation.
* A committed reply is visible to a waiting caller across separate short
  transactions, and once consumed the command is no longer delivered by the
  work poll.
"""

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
REPLY_KEYS = ('outcome', 'command')


class _AgentChannelReplyBase(base.DbTestCase):
    """Shared journal setup for the consumption tests."""

    def setUp(self):
        super(_AgentChannelReplyBase, self).setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()
        self.execution_id = uuidutils.generate_uuid()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, self.execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        self.session_uuid = self.session['uuid']
        self.fence = self.session['fence']
        self.command = self.dbapi.publish_agent_command(
            self.session_uuid, EPOCH, 'step-1', NAME, {}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, self.fence)
        self.command_uuid = self.command['uuid']

    def _report(self, kind, payload):
        return self.dbapi.report_agent_channel(
            self.session_uuid,
            [{'command_id': self.command_uuid,
              'workflow_epoch': EPOCH,
              'kind': kind,
              'payload': payload}])

    def _commit_reply(self):
        self._report('receipt', {'value': 'received'})
        self._report('result', {'value': 'ok'})

    def _command_state(self, command_uuid):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelCommand.state).where(
                    models.AgentChannelCommand.uuid == command_uuid)
            return conn.execute(stmt).scalar_one()

    def _require_reply_result(self, result, context):
        for key in REPLY_KEYS:
            self.assertIn(key, result, '%s result lacks key %s'
                          % (context, key))

    def _reserve_node(self):
        self.dbapi.update_node(self.node.id, {'reservation': 'test-conductor'})


class AgentChannelConsumeTestCase(_AgentChannelReplyBase):

    def _consume(self, node_uuid=None, session_uuid=None,
                 command_uuid=None, epoch=EPOCH, fence=None):
        consume = getattr(self.dbapi, 'consume_agent_channel_command', None)
        self.assertTrue(
            callable(consume),
            'dbapi.consume_agent_channel_command is missing or not '
            'callable; the agent channel reply consumption API is not '
            'implemented yet')
        command_uuid = command_uuid or self.command_uuid
        return consume(node_uuid or self.node.uuid,
                       session_uuid or self.session_uuid,
                       command_uuid, epoch,
                       self.fence if fence is None else fence)

    def test_consume_requires_a_committed_result(self):
        with self.assertRaises(exception.AgentChannelCommandNotReady):
            self._consume()
        self.assertEqual('queued', self._command_state(self.command_uuid))
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        self.assertIsNone(stored['result'])

    def test_consume_returns_the_stored_reply_and_marks_consumed(self):
        self._commit_reply()
        result = self._consume()
        self._require_reply_result(result, 'consume')
        self.assertEqual('reply', result['outcome'])
        command = result['command']
        self.assertEqual(self.command_uuid, command['uuid'])
        self.assertEqual(self.session_uuid, command['session_uuid'])
        self.assertEqual('consumed', command['state'])
        self.assertEqual({'value': 'ok'}, command['result'])
        self.assertEqual({'value': 'received'}, command['receipt'])
        # The journal row really is consumed, in one transition.
        self.assertEqual('consumed', self._command_state(self.command_uuid))

    def test_consume_is_idempotent_and_never_double_advances(self):
        self._commit_reply()
        first = self._consume()
        second = self._consume()
        self._require_reply_result(second, 'second consume')
        self.assertEqual('reply', second['outcome'])
        # Both calls return the identical stored reply and state; nothing
        # advances a second time.
        self.assertEqual(first['command'], second['command'])
        self.assertEqual('consumed', self._command_state(self.command_uuid))

    def test_consume_with_a_stale_fence_is_rejected_without_consuming(self):
        self._commit_reply()
        # A new execution process supersedes the session and raises the
        # fence. The old fence must not consume anything.
        self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._consume()
        self.assertEqual('queued', self._command_state(self.command_uuid))
        self.assertEqual({'value': 'ok'},
                         self.dbapi.get_agent_channel_command(
                             self.command_uuid)['result'])

    def test_consume_with_a_stale_epoch_is_rejected_without_consuming(self):
        self._commit_reply()
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._consume(epoch=EPOCH + 1)
        self.assertEqual('queued', self._command_state(self.command_uuid))

    def test_consume_of_a_command_of_another_node_is_fenced(self):
        other_node = db_utils.create_test_node(uuid=uuidutils.generate_uuid())
        other_session = self.dbapi.activate_agent_channel(
            other_node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        other_command = self.dbapi.publish_agent_command(
            other_session['uuid'], EPOCH, 'step-1', NAME, {}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, other_session['fence'])
        # The command of the other node belongs to a session this node's
        # head does not own.
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._consume(session_uuid=other_session['uuid'],
                          command_uuid=other_command['uuid'],
                          fence=other_session['fence'])
        # Its own node can consume it: the fence is per node.
        self.dbapi.report_agent_channel(
            other_session['uuid'],
            [{'command_id': other_command['uuid'],
              'workflow_epoch': EPOCH,
              'kind': 'result',
              'payload': {'value': 'other'}}])
        own = self.dbapi.consume_agent_channel_command(
            other_node.uuid, other_session['uuid'], other_command['uuid'],
            EPOCH, other_session['fence'])
        self.assertEqual('reply', own['outcome'])
        self.assertEqual({'value': 'other'}, own['command']['result'])

    def test_consume_unknown_command_raises_not_found(self):
        with self.assertRaises(exception.NotFound):
            self._consume(command_uuid=uuidutils.generate_uuid())

    def test_consume_unknown_session_raises_not_found(self):
        with self.assertRaises(exception.NotFound):
            self._consume(session_uuid=uuidutils.generate_uuid())

    def test_consume_makes_no_node_update_and_no_epoch_advance(self):
        self._commit_reply()
        before = self.dbapi.get_node_by_uuid(self.node.uuid)
        head = enginefacade.writer.get_engine()
        with head.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelHead.workflow_epoch,
                models.AgentChannelHead.fence).where(
                    models.AgentChannelHead.node_id == self.node.id)
            head_before = conn.execute(stmt).one()
        self._consume()
        after = self.dbapi.get_node_by_uuid(self.node.uuid)
        self.assertEqual(before.reservation, after.reservation)
        self.assertEqual(before.provision_state, after.provision_state)
        with head.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelHead.workflow_epoch,
                models.AgentChannelHead.fence).where(
                    models.AgentChannelHead.node_id == self.node.id)
            head_after = conn.execute(stmt).one()
        self.assertEqual(head_before, head_after)

    def test_consume_succeeds_while_the_node_holds_an_exclusive_reservation(
            self):
        # An exclusive reservation is a committed Node field, the same shape
        # the conductor task fence leaves behind. The consume path must not
        # need the Node-row lock or update.
        self._reserve_node()
        before = self.dbapi.get_node_by_uuid(self.node.uuid)
        self._commit_reply()
        result = self._consume()
        self.assertEqual('reply', result['outcome'])
        after = self.dbapi.get_node_by_uuid(self.node.uuid)
        self.assertEqual('test-conductor', after.reservation)
        self.assertEqual(before.provision_state, after.provision_state)

    def test_consumed_command_is_no_longer_delivered_by_the_work_poll(self):
        self._commit_reply()
        self._consume()
        poll = self.dbapi.poll_agent_channel_work(
            self.node.uuid, self.session_uuid, self.execution_id, EPOCH)
        self.assertEqual('no_work', poll['outcome'])
        self.assertIsNone(poll['command'])


class AgentChannelWaitTestCase(_AgentChannelReplyBase):

    def _wait(self, timeout=2.0, wake_hint=None, wait_interval=0.01,
              session_uuid=None, command_uuid=None, epoch=EPOCH, fence=None):
        wait_reply = getattr(self.dbapi, 'wait_agent_channel_reply', None)
        self.assertTrue(
            callable(wait_reply),
            'dbapi.wait_agent_channel_reply is missing or not callable; '
            'the reservation-safe reply wait is not implemented yet')
        return wait_reply(
            self.node.uuid, session_uuid or self.session_uuid,
            command_uuid or self.command_uuid, epoch,
            self.fence if fence is None else fence, timeout,
            wake_hint=wake_hint, wait_interval=wait_interval)

    def test_wait_returns_the_reply_once_it_is_committed(self):
        calls = []

        def commit_when_hinted():
            calls.append(True)
            self._commit_reply()

        result = self._wait(wake_hint=commit_when_hinted)
        self._require_reply_result(result, 'wait')
        self.assertEqual('reply', result['outcome'])
        command = result['command']
        self.assertEqual(self.command_uuid, command['uuid'])
        self.assertEqual('consumed', command['state'])
        self.assertEqual({'value': 'ok'}, command['result'])
        # The reply was committed by a separate dispatcher transaction while
        # the wait was between its short journal reads.
        self.assertTrue(calls)
        self.assertEqual('consumed', self._command_state(self.command_uuid))

    def test_wait_times_out_with_no_result_and_leaves_the_command_queued(self):
        result = self._wait(timeout=0.2, wait_interval=0.01)
        self._require_reply_result(result, 'wait')
        self.assertEqual('no_reply', result['outcome'])
        self.assertIsNone(result['command'])
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        self.assertEqual('queued', stored['state'])
        self.assertIsNone(stored['result'])

    def test_wait_wakes_while_the_node_holds_an_exclusive_reservation(self):
        self._reserve_node()
        before = self.dbapi.get_node_by_uuid(self.node.uuid)
        calls = []

        def commit_when_hinted():
            calls.append(True)
            self._commit_reply()

        result = self._wait(wake_hint=commit_when_hinted)
        self.assertEqual('reply', result['outcome'])
        self.assertTrue(calls)
        # The wait itself never cleared or otherwise changed the caller's
        # exclusive reservation or any Node field.
        after = self.dbapi.get_node_by_uuid(self.node.uuid)
        self.assertEqual('test-conductor', after.reservation)
        self.assertEqual(before.provision_state, after.provision_state)

    def test_wait_receives_no_result_when_the_session_is_superseded(self):
        # A new execution process supersedes the old session. The old
        # session keeps its late-report authority, so the result is stored
        # in the journal; the wait must still fence the consume.
        self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)
        self._commit_reply()
        result = self._wait(timeout=0.2, wait_interval=0.01)
        self._require_reply_result(result, 'wait')
        self.assertEqual('no_reply', result['outcome'])
        self.assertIsNone(result['command'])
        stored = self.dbapi.get_agent_channel_command(self.command_uuid)
        self.assertEqual('queued', stored['state'])
        # The late result stays stored for the report-only acknowledgment
        # path; it did not advance anything.
        self.assertEqual({'value': 'ok'}, stored['result'])

    def test_wait_unknown_command_raises_not_found(self):
        with self.assertRaises(exception.NotFound):
            self._wait(command_uuid=uuidutils.generate_uuid(),
                       timeout=0.2, wait_interval=0.01)
