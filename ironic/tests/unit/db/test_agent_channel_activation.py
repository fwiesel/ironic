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

"""Unit tests for agent channel session activation and fencing.

The journal seed in ``test_agent_channel_journal`` pins command identity
and publication-time supersession. These tests pin the activation side of
that contract: a repeated activation of the same execution process is
idempotent, supersession is scoped to one node, and a publisher that is not
the current session owner is rejected with a fence conflict.
"""

from unittest import mock

from oslo_db.sqlalchemy import enginefacade
from oslo_utils import uuidutils
import sqlalchemy

from ironic.common import exception
from ironic.db.sqlalchemy import api as sqlalchemy_api
from ironic.db.sqlalchemy import models
from ironic.tests.unit.db import base
from ironic.tests.unit.db import utils as db_utils

TOKEN_VERIFIER = 'deadbeef' * 8
OWNER_INCARNATION = 'incarnation-1'
PROFILE = 'agent-poll'
EPOCH = 1
NAME = 'power_on'
BOUNDARY = 'completion'


class AgentChannelActivationTestCase(base.DbTestCase):

    def setUp(self):
        super(AgentChannelActivationTestCase, self).setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()

    def _activate(self, node_uuid=None, execution_id=None, epoch=EPOCH):
        return self.dbapi.activate_agent_channel(
            node_uuid or self.node.uuid,
            execution_id or uuidutils.generate_uuid(),
            epoch, self.conductor.id, OWNER_INCARNATION, TOKEN_VERIFIER,
            PROFILE)

    def _publish(self, session, invocation_key='step-1', epoch=EPOCH):
        return self.dbapi.publish_agent_command(
            session['uuid'], epoch, invocation_key, NAME, {}, BOUNDARY,
            self.conductor.id, OWNER_INCARNATION, session['fence'])

    def _session_state(self, session_uuid):
        engine = enginefacade.writer.get_engine()
        with engine.connect() as conn:
            stmt = sqlalchemy.select(
                models.AgentChannelSession.state).where(
                    models.AgentChannelSession.uuid == session_uuid)
            return conn.execute(stmt).scalar_one()

    def test_reactivation_of_same_execution_is_idempotent(self):
        execution_id = uuidutils.generate_uuid()
        first = self._activate(execution_id=execution_id)
        second = self._activate(execution_id=execution_id)
        self.assertEqual(first['uuid'], second['uuid'])
        self.assertEqual(first['fence'], second['fence'])
        # A repeated activation must not fence out the running session.
        command = self._publish(first)
        self.assertEqual('queued', command['state'])

    def test_new_activation_supersedes_previous_session(self):
        first = self._activate()
        command = self._publish(first)
        second = self._activate()
        self.assertNotEqual(first['uuid'], second['uuid'])
        self.assertGreater(second['fence'], first['fence'])
        self.assertEqual('draining', self._session_state(first['uuid']))
        self.assertEqual('active', self._session_state(second['uuid']))
        # The superseded session keeps its journal rows.
        self.assertEqual(
            command['uuid'],
            self.dbapi.get_agent_channel_command(command['uuid'])['uuid'])

    def test_fence_is_scoped_to_one_node(self):
        other_node = db_utils.create_test_node(
            uuid=uuidutils.generate_uuid())
        first = self._activate()
        other = self._activate(node_uuid=other_node.uuid)
        # Activating a second node must not supersede the first node.
        self.assertEqual('active', self._session_state(first['uuid']))
        self._publish(first)
        self._publish(other)

    def test_publish_with_wrong_fence_raises_fence_conflict(self):
        session = self._activate()
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self.dbapi.publish_agent_command(
                session['uuid'], EPOCH, 'step-1', NAME, {}, BOUNDARY,
                self.conductor.id, OWNER_INCARNATION,
                session['fence'] + 1)

    def test_publish_with_stale_epoch_raises_fence_conflict(self):
        session = self._activate()
        with self.assertRaises(exception.AgentChannelFenceConflict):
            self._publish(session, epoch=EPOCH + 1)

    def test_session_reference_does_not_expose_token_verifier(self):
        session = self._activate()
        self.assertNotIn('token_verifier', session)
        self.assertNotIn(TOKEN_VERIFIER, str(session))

    def test_concurrent_duplicate_publication_returns_stored_command(self):
        session = self._activate()
        first = self._publish(session)
        real_lookup = sqlalchemy_api._find_agent_channel_command
        lookups = []

        def lookup(*args, **kwargs):
            # Simulate a competing publisher whose row becomes visible
            # between our identity lookup and our insert.
            if not lookups:
                lookups.append(True)
                return None
            return real_lookup(*args, **kwargs)

        with mock.patch.object(sqlalchemy_api,
                               '_find_agent_channel_command',
                               autospec=True,
                               side_effect=lookup):
            second = self._publish(session)
        self.assertEqual(first['uuid'], second['uuid'])
        self.assertEqual(first['sequence'], second['sequence'])
