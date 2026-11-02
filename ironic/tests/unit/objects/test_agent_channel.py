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
travel over RPC during a rolling upgrade. This test pins the invariants that
keep the record layer consistent with the persistence layer: the objects
register, the release mapping knows their version, and their fields do not
drift from the journal columns. It also pins the command record's versioning
rules: converting to 1.0 removes the 1.1 report slots, dehydrating for a
1.0 peer drops them, and a 1.0 representation hydrates into a 1.1 record
with the slots unset.
"""

import copy
import unittest
import uuid

from ironic.common import release_mappings
from ironic.conf import CONF
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

# The report slots the command record grew at version 1.1. A 1.0 peer
# predates them, so no 1.0 representation may carry them.
REPORT_SLOTS = ('receipt', 'acceptance', 'result')

# The fields the command record carried at version 1.0.
V1_0_FIELDS = ('id', 'uuid', 'session_uuid', 'workflow_epoch',
               'invocation_key', 'sequence', 'name', 'params',
               'reply_boundary', 'params_sha256', 'state')


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

    def _make_command(self, with_slots=True):
        cmd = agent_channel.AgentChannelCommand()
        cmd.id = 1
        cmd.uuid = str(uuid.uuid4())
        cmd.session_uuid = str(uuid.uuid4())
        cmd.workflow_epoch = 1
        cmd.invocation_key = 'work-1'
        cmd.sequence = 3
        cmd.name = 'power'
        cmd.params = {'method': 'on'}
        cmd.reply_boundary = 'report'
        cmd.params_sha256 = 'a' * 64
        cmd.state = 'admitted'
        if with_slots:
            cmd.receipt = {'seq': 3}
            cmd.acceptance = {'accepted': True}
            cmd.result = {'code': 200}
        return cmd

    def test_downgrade_to_1_0_removes_populated_report_slots(self):
        cmd = self._make_command()
        cmd.convert_to_version('1.0')
        self.assertEqual('1.0', cmd.VERSION)
        for slot in REPORT_SLOTS:
            self.assertFalse(cmd.obj_attr_is_set(slot),
                             '%s must not survive a downgrade to 1.0'
                             % slot)
        for field in V1_0_FIELDS:
            self.assertTrue(cmd.obj_attr_is_set(field),
                            '%s must survive a downgrade to 1.0' % field)

    def test_downgrade_to_1_0_is_safe_when_slots_are_unset(self):
        # The slots are nullable; a record that never got a report must
        # downgrade to 1.0 without error.
        cmd = self._make_command(with_slots=False)
        cmd.convert_to_version('1.0')
        self.assertEqual('1.0', cmd.VERSION)
        for slot in REPORT_SLOTS:
            self.assertFalse(cmd.obj_attr_is_set(slot),
                             '%s must stay unset after a downgrade to 1.0'
                             % slot)
        self.assertEqual('power', cmd.name)
        self.assertEqual(3, cmd.sequence)

    def test_downgrade_for_db_clears_slots_and_keeps_old_fields(self):
        cmd = self._make_command()
        old_fields = copy.deepcopy({field: getattr(cmd, field)
                                    for field in V1_0_FIELDS})
        cmd.convert_to_version('1.0', remove_unavailable_fields=False)
        self.assertEqual('1.0', cmd.VERSION)
        # FlexibleDictField exposes a cleared nullable value as an empty dict.
        for slot in REPORT_SLOTS:
            self.assertTrue(cmd.obj_attr_is_set(slot))
            self.assertEqual({}, getattr(cmd, slot))
        self.assertEqual(old_fields, {field: getattr(cmd, field)
                                      for field in V1_0_FIELDS})

    def test_primitive_for_target_1_0_drops_report_slots(self):
        cmd = self._make_command()
        prim = cmd.obj_to_primitive(target_version='1.0')
        self.assertEqual('1.0', prim['ironic_object.version'])
        data = prim['ironic_object.data']
        self.assertEqual(set(V1_0_FIELDS), set(data),
                         'a 1.0 primitive must carry only 1.0 fields')
        changes = prim.get('ironic_object.changes', [])
        for slot in REPORT_SLOTS:
            self.assertNotIn(slot, changes,
                             '%s must not be announced as changed at 1.0'
                             % slot)
        self.assertEqual({'method': 'on'}, data['params'])

    def test_primitive_at_1_1_keeps_report_slots_and_old_fields(self):
        cmd = self._make_command()
        prim = cmd.obj_to_primitive(target_version='1.1')
        self.assertEqual('1.1', prim['ironic_object.version'])
        data = prim['ironic_object.data']
        self.assertEqual(set(V1_0_FIELDS) | set(REPORT_SLOTS), set(data))
        self.assertEqual({'seq': 3}, data['receipt'])
        self.assertEqual({'accepted': True}, data['acceptance'])
        self.assertEqual({'code': 200}, data['result'])
        self.assertEqual({'method': 'on'}, data['params'])

    def test_server_serializer_keeps_1_1_without_a_release_pin(self):
        # Master is not a valid release pin. Use the valid unpinned setting
        # to check that the server preserves the current command fields.
        cmd = self._make_command()
        self.config(pin_release_version=None)
        serializer = obj_base.IronicObjectSerializer(is_server=True)
        entity = serializer.serialize_entity(None, cmd)
        self.assertEqual('1.1', entity['ironic_object.version'])
        data = entity['ironic_object.data']
        self.assertEqual(set(V1_0_FIELDS) | set(REPORT_SLOTS), set(data))
        self.assertEqual({'seq': 3}, data['receipt'])
        self.assertEqual({'accepted': True}, data['acceptance'])
        self.assertEqual({'code': 200}, data['result'])

    def test_hydrating_a_1_0_primitive_leaves_slots_unset(self):
        # A peer that only knows 1.0 sends none of the slot keys; the
        # hydrating side must absorb that into a 1.1 record cleanly.
        command_uuid = str(uuid.uuid4())
        session_uuid = str(uuid.uuid4())
        entity = {
            'ironic_object.name': 'AgentChannelCommand',
            'ironic_object.namespace': 'ironic',
            'ironic_object.version': '1.0',
            'ironic_object.data': {
                'id': 1,
                'uuid': command_uuid,
                'session_uuid': session_uuid,
                'workflow_epoch': 1,
                'invocation_key': 'work-1',
                'sequence': 3,
                'name': 'power',
                'params': {'method': 'on'},
                'reply_boundary': 'report',
                'params_sha256': 'a' * 64,
                'state': 'admitted',
            },
            'ironic_object.changes': list(V1_0_FIELDS),
        }
        serializer = obj_base.IronicObjectSerializer(is_server=True)
        obj = serializer.deserialize_entity(None, entity)
        self.assertIsInstance(obj, agent_channel.AgentChannelCommand)
        self.assertEqual('1.1', obj.VERSION)
        for slot in REPORT_SLOTS:
            self.assertFalse(obj.obj_attr_is_set(slot),
                             '%s must stay unset when hydrated from 1.0'
                             % slot)
        self.assertEqual(command_uuid, obj.uuid)
        self.assertEqual({'method': 'on'}, obj.params)


class AgentChannelSerializerCleanupTestCase(base.TestCase):

    def test_server_serializer_leaves_release_pin_configurable(self):
        # Run the serializer case with its complete fixture lifecycle.
        case = AgentChannelObjectTestCase(
            'test_server_serializer_keeps_1_1_without_a_release_pin')
        result = unittest.TestResult()
        case.run(result)
        self.assertTrue(result.wasSuccessful(),
                        result.errors + result.failures)
        self.assertIsNone(CONF.pin_release_version)
        pin = release_mappings.RELEASE_VERSIONS[-1]
        self.config(pin_release_version=pin)
        self.assertEqual(pin, CONF.pin_release_version)
        self.config(pin_release_version=None)
        self.assertIsNone(CONF.pin_release_version)
