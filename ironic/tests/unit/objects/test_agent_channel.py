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

"""Tests for the versioned agent channel journal records.

The frozen contract stores the journal as three record kinds: ``head``,
``session``, and ``command``. The records are versioned objects, so they can
travel over RPC during a rolling upgrade. This test pins the three
invariants that keep the record layer consistent with the persistence layer:
the objects register, the release mapping knows their version, and their
fields do not drift from the journal columns.
"""

from ironic.common import release_mappings
from ironic.db.sqlalchemy import models
from ironic.objects import agent_channel
from ironic.objects import base as obj_base
from ironic.tests import base

# Map each journal record to its object class and its DB model. The dict
# order is the contract order: head, session, command.
JOURNAL_RECORDS = {
    'AgentChannelHead': (agent_channel.AgentChannelHead,
                         models.AgentChannelHead),
    'AgentChannelSession': (agent_channel.AgentChannelSession,
                            models.AgentChannelSession),
    'AgentChannelCommand': (agent_channel.AgentChannelCommand,
                            models.AgentChannelCommand),
}

# Columns the versioned object layer owns. ``version`` is the optimistic
# lock column of the DB model; the object carries its own version.
OBJECT_OWNED_COLUMNS = {'version'}


class AgentChannelObjectTestCase(base.TestCase):

    def test_records_are_registered_as_versioned_objects(self):
        registered = obj_base.IronicObjectRegistry.obj_classes()
        for name in JOURNAL_RECORDS:
            self.assertIn(name, registered,
                          '%s is not a registered versioned object'
                          % name)

    def test_records_have_consistent_versions(self):
        # The head and session records are new at 1.0. The command record
        # grew its report slots, so it advanced to 1.1 while the record
        # family is still unreleased.
        expect_start = {
            'AgentChannelHead': '1.0',
            'AgentChannelSession': '1.0',
            'AgentChannelCommand': '1.1',
        }
        for name, (obj_cls, _model) in JOURNAL_RECORDS.items():
            self.assertEqual(expect_start[name], obj_cls.VERSION,
                             '%s has an unexpected version' % name)

    def test_master_release_mapping_knows_the_records(self):
        objects = release_mappings.RELEASE_MAPPING['master']['objects']
        for name, (obj_cls, _model) in JOURNAL_RECORDS.items():
            self.assertIn(name, objects,
                          '%s is missing from the master release mapping'
                          % name)
            self.assertIn(obj_cls.VERSION, objects[name],
                          'the master release mapping lacks version %s of %s'
                          % (obj_cls.VERSION, name))

    def test_record_fields_track_the_journal_columns(self):
        for name, (obj_cls, model) in JOURNAL_RECORDS.items():
            column_names = {column.name
                            for column in model.__table__.columns}
            expected = column_names - OBJECT_OWNED_COLUMNS
            self.assertEqual(expected, set(obj_cls.fields),
                             '%s fields drifted from %s columns'
                             % (name, model.__tablename__))
