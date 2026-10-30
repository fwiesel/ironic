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

"""Unit tests for agent channel publication ownership.

The journal seed in ``test_agent_channel_journal`` pins command identity
and ``test_agent_channel_activation`` pins the fence generation and epoch
checks at publication. These tests pin the remaining publication
authority rule: the journal head stores the owner conductor process
incarnation and the fencing generation of the current authority. A
publisher that presents a valid session UUID, fence, and epoch but a
foreign owner conductor ID or owner incarnation holds no work
authority. It must be rejected with a distinct
``AgentChannelFenceConflict`` before it can create a command or replay
a stored one. A rejected publication changes no command row, no
sequence, and no head or session field.
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
FOREIGN_INCARNATION = 'incarnation-2'
PROFILE = 'agent-poll'
EPOCH = 1
NAME = 'power_on'
BOUNDARY = 'completion'


class AgentChannelPublicationTestCase(base.DbTestCase):

    def setUp(self):
        super(AgentChannelPublicationTestCase, self).setUp()
        self.node = db_utils.create_test_node()
        self.owner = db_utils.create_test_conductor(
            hostname='owner-conductor')
        self.foreign = db_utils.create_test_conductor(
            hostname='foreign-conductor')
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, uuidutils.generate_uuid(), EPOCH,
            self.owner.id, OWNER_INCARNATION, TOKEN_VERIFIER, PROFILE)

    def _publish(self, owner=None, incarnation=None, key='step-1'):
        return self.dbapi.publish_agent_command(
            self.session['uuid'], EPOCH, key, NAME, {}, BOUNDARY,
            self.owner.id if owner is None else owner,
            OWNER_INCARNATION if incarnation is None else incarnation,
            self.session['fence'])

    def _head_row(self):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(models.AgentChannelHead).where(
                models.AgentChannelHead.node_id == self.node.id)
            return dict(conn.execute(stmt).mappings().one())

    def _session_row(self):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(models.AgentChannelSession).where(
                models.AgentChannelSession.uuid == self.session['uuid'])
            return dict(conn.execute(stmt).mappings().one())

    def _command_count(self):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(models.AgentChannelCommand).where(
                models.AgentChannelCommand.session_uuid
                == self.session['uuid'])
            return len(conn.execute(stmt).scalars().all())

    def _assert_no_publication(self):
        self.assertEqual(self._command_count(), 0)
        self.assertEqual(self._session_row()['next_sequence'], 0)

    def test_publish_with_wrong_conductor_id_raises_fence_conflict(self):
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(owner=self.foreign.id)
        self._assert_no_publication()

    def test_publish_with_wrong_incarnation_raises_fence_conflict(self):
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(incarnation=FOREIGN_INCARNATION)
        self._assert_no_publication()

    def test_publish_with_both_owner_values_wrong_raises_fence_conflict(
            self):
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(owner=self.foreign.id,
                          incarnation=FOREIGN_INCARNATION)
        self._assert_no_publication()

    def test_wrong_owner_cannot_replay_a_stored_command(self):
        stored = self._publish()
        self.assertEqual(stored['state'], 'queued')
        # The same frozen identity under a foreign authority is a fence
        # conflict, not a replay of the stored command.
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(owner=self.foreign.id)
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(incarnation=FOREIGN_INCARNATION)
        command = self.dbapi.get_agent_channel_command(stored['uuid'])
        self.assertEqual(command['uuid'], stored['uuid'])
        self.assertEqual(command['sequence'], stored['sequence'])
        self.assertEqual(command['params_sha256'], stored['params_sha256'])
        self.assertEqual(self._command_count(), 1)
        self.assertEqual(self._session_row()['next_sequence'], 1)

    def test_correct_owner_publish_and_repeat_keep_identity(self):
        first = self._publish()
        self.assertEqual(first['state'], 'queued')
        self.assertEqual(first['sequence'], 1)
        repeat = self._publish()
        self.assertEqual(repeat['uuid'], first['uuid'])
        self.assertEqual(repeat['sequence'], first['sequence'])
        self.assertEqual(self._command_count(), 1)

    def test_rejected_publish_changes_no_head_or_session_field(self):
        head_before = self._head_row()
        session_before = self._session_row()
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(owner=self.foreign.id)
        self.assertEqual(self._head_row(), head_before)
        self.assertEqual(self._session_row(), session_before)
