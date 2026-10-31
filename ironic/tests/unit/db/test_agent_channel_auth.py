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

"""Unit tests for agent channel token verification in the journal.

The wire ``work`` request names its session and execution, so the token
authenticates that named pair. The wire ``reports`` request names only the
node: the agent submits a bounded list whose entries carry command IDs, so
the reporting session is identified by its token alone. Both checks compare
a SHA256 of the presented token with the session's stored verifier using a
constant-time compare. The verifier itself never leaves the journal.

DB API contract under test, all on ``dbapi.get_instance()``:

* ``verify_agent_channel_token(node_uuid, session_uuid, execution_id,
  agent_token)`` returns True only when the node exists, the session exists
  for that node and execution, and the SHA256 of the presented token equals
  the session's stored verifier under a constant-time compare. An unknown
  node raises ``exception.NodeNotFound``; every other mismatch returns False
  and reveals no journal identity.
* ``find_agent_channel_session(node_uuid, agent_token)`` returns the UUID of
  a session of the node whose stored verifier matches the presented token,
  or None. An unknown node raises ``exception.NodeNotFound``.
"""

import hashlib

from oslo_utils import uuidutils

from ironic.common import exception
from ironic.tests.unit.db import base
from ironic.tests.unit.db import utils as db_utils

TOKEN = 'ramdisk-token-alfalfa'
VERIFIER = hashlib.sha256(TOKEN.encode('utf-8')).hexdigest()
OTHER_TOKEN = 'ramdisk-token-broccoli'
OTHER_VERIFIER = hashlib.sha256(OTHER_TOKEN.encode('utf-8')).hexdigest()
OWNER_INCARNATION = 'incarnation-1'
PROFILE = 'agent-poll'
EPOCH = 1


class AgentChannelTokenTestCase(base.DbTestCase):

    def setUp(self):
        super(AgentChannelTokenTestCase, self).setUp()
        self.node = db_utils.create_test_node()
        self.conductor = db_utils.create_test_conductor()
        self.execution_id = uuidutils.generate_uuid()
        self.session = self.dbapi.activate_agent_channel(
            self.node.uuid, self.execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, VERIFIER, PROFILE)
        self.session_uuid = self.session['uuid']

    def _activate(self, node_uuid, execution_id, verifier=VERIFIER):
        return self.dbapi.activate_agent_channel(
            node_uuid, execution_id, EPOCH,
            self.conductor.id, OWNER_INCARNATION, verifier, PROFILE)

    def _verify(self, session_uuid=None, execution_id=None, token=TOKEN):
        return self.dbapi.verify_agent_channel_token(
            self.node.uuid,
            session_uuid or self.session_uuid,
            execution_id or self.execution_id,
            token)

    def test_verify_matches_the_named_session(self):
        self.assertTrue(self._verify())

    def test_verify_rejects_a_wrong_token(self):
        self.assertFalse(self._verify(token=OTHER_TOKEN))

    def test_verify_rejects_a_missing_token(self):
        self.assertFalse(self._verify(token=None))

    def test_verify_rejects_a_session_of_another_node(self):
        other_node = db_utils.create_test_node()
        other_session = self._activate(other_node.uuid,
                                       uuidutils.generate_uuid())
        self.assertFalse(self._verify(session_uuid=other_session['uuid']))

    def test_verify_rejects_an_unknown_session(self):
        self.assertFalse(self._verify(session_uuid=uuidutils.generate_uuid()))

    def test_verify_rejects_a_wrong_execution(self):
        self.assertFalse(self._verify(execution_id=uuidutils.generate_uuid()))

    def test_verify_unknown_node_raises_not_found(self):
        self.assertRaises(
            exception.NotFound, self.dbapi.verify_agent_channel_token,
            str(uuidutils.generate_uuid()), self.session_uuid,
            self.execution_id, TOKEN)

    def test_find_identifies_the_session_by_token(self):
        found = self.dbapi.find_agent_channel_session(self.node.uuid, TOKEN)
        self.assertEqual(self.session_uuid, found)

    def test_find_does_not_match_an_unknown_token(self):
        self.assertIsNone(
            self.dbapi.find_agent_channel_session(self.node.uuid, OTHER_TOKEN))

    def test_find_does_not_match_a_missing_token(self):
        self.assertIsNone(
            self.dbapi.find_agent_channel_session(self.node.uuid, None))

    def test_find_selects_among_multiple_sessions(self):
        second_execution = uuidutils.generate_uuid()
        second = self._activate(self.node.uuid, second_execution,
                                verifier=OTHER_VERIFIER)
        # The first session is superseded but keeps its journal rows and its
        # late-report authority, so both tokens resolve to their own session.
        self.assertEqual(self.session_uuid,
                         self.dbapi.find_agent_channel_session(
                             self.node.uuid, TOKEN))
        self.assertEqual(second['uuid'],
                         self.dbapi.find_agent_channel_session(
                             self.node.uuid, OTHER_TOKEN))

    def test_find_unknown_node_raises_not_found(self):
        self.assertRaises(
            exception.NotFound, self.dbapi.find_agent_channel_session,
            str(uuidutils.generate_uuid()), TOKEN)

    def test_sessions_of_another_node_are_not_visible(self):
        other_node = db_utils.create_test_node()
        self._activate(other_node.uuid, uuidutils.generate_uuid(),
                       verifier=OTHER_VERIFIER)
        # The other node's session must not resolve for this node's token.
        self.assertEqual(self.session_uuid,
                         self.dbapi.find_agent_channel_session(
                             self.node.uuid, TOKEN))
        self.assertIsNone(
            self.dbapi.find_agent_channel_session(other_node.uuid, TOKEN))
