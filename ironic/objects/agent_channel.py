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

"""Versioned records of the agent channel journal.

The journal keeps three record kinds in the database. A ``head`` owns the
agent channel of one node and holds the node's fence generation. A
``session`` is one agent execution process. A ``command`` is one frozen IPA
method invocation. The records are versioned objects so that a rolling
upgrade can carry them over RPC.

The records are plain data here. Publication, admission, and reporting are
transactions of the journal DB API, and they build these shapes from the
stored rows.
"""

from oslo_utils import versionutils
from oslo_versionedobjects import base as object_base

from ironic.objects import base
from ironic.objects import fields as object_fields

REPORT_SLOTS = ('receipt', 'acceptance', 'result')


@base.IronicObjectRegistry.register
class AgentChannelHead(base.IronicObject,
                       object_base.VersionedObjectDictCompat):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': object_fields.IntegerField(),
        'node_id': object_fields.IntegerField(),
        'workflow_epoch': object_fields.IntegerField(default=0),
        'session_uuid': object_fields.UUIDField(nullable=True),
        'owner_conductor_id': object_fields.IntegerField(nullable=True),
        'owner_incarnation': object_fields.StringField(nullable=True),
        'fence': object_fields.IntegerField(default=0),
    }


@base.IronicObjectRegistry.register
class AgentChannelSession(base.IronicObject,
                          object_base.VersionedObjectDictCompat):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': object_fields.IntegerField(),
        'uuid': object_fields.UUIDField(nullable=False),
        'node_id': object_fields.IntegerField(),
        'execution_id': object_fields.UUIDField(nullable=False),
        'workflow_epoch': object_fields.IntegerField(),
        'fence': object_fields.IntegerField(),
        'owner_conductor_id': object_fields.IntegerField(nullable=True),
        'owner_incarnation': object_fields.StringField(nullable=True),
        # NOTE: This is the verifier of the agent token, never the token.
        'token_verifier': object_fields.StringField(nullable=False),
        'profile': object_fields.StringField(nullable=False),
        'state': object_fields.StringField(nullable=False),
        'next_sequence': object_fields.IntegerField(default=0),
    }


@base.IronicObjectRegistry.register
class AgentChannelCommand(base.IronicObject,
                          object_base.VersionedObjectDictCompat):
    # Version 1.0: Initial version
    # Version 1.1: Added the receipt, acceptance, and result report slots
    VERSION = '1.1'

    fields = {
        'id': object_fields.IntegerField(),
        'uuid': object_fields.UUIDField(nullable=False),
        'session_uuid': object_fields.UUIDField(nullable=False),
        'workflow_epoch': object_fields.IntegerField(),
        'invocation_key': object_fields.StringField(nullable=False),
        'sequence': object_fields.IntegerField(),
        'name': object_fields.StringField(nullable=False),
        'params': object_fields.FlexibleDictField(nullable=False),
        'reply_boundary': object_fields.StringField(nullable=False),
        'params_sha256': object_fields.StringField(nullable=False),
        'state': object_fields.StringField(nullable=False),
        'receipt': object_fields.FlexibleDictField(nullable=True),
        'acceptance': object_fields.FlexibleDictField(nullable=True),
        'result': object_fields.FlexibleDictField(nullable=True),
    }

    def _convert_to_version(self, target_version,
                            remove_unavailable_fields=True):
        """Convert to the target version.

        Version 1.1 added the receipt, acceptance, and result report slots.
        A 1.0 peer predates them, so they must not be sent to it. For DB
        interactions the slots are cleared to their default instead of being
        removed, so that the stored row stays consistent with the columns.

        :param target_version: the desired version of the object
        :param remove_unavailable_fields: True to remove fields that are
            unavailable in the target version; set this to True when
            (de)serializing. False to set the unavailable fields to their
            default values; set this to False for DB interactions.
        """
        target_version = versionutils.convert_version_to_tuple(target_version)
        if target_version < (1, 1):
            for field in REPORT_SLOTS:
                if not self.obj_attr_is_set(field):
                    continue
                if remove_unavailable_fields:
                    delattr(self, field)
                else:
                    setattr(self, field, None)

    def obj_make_compatible(self, primitive, target_version):
        """Strip the 1.1 report slots from a 1.0 representation."""
        target_version = versionutils.convert_version_to_tuple(target_version)
        if target_version < (1, 1):
            for field in REPORT_SLOTS:
                primitive.pop(field, None)
