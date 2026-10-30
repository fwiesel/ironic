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

from oslo_versionedobjects import base as object_base

from ironic.objects import base
from ironic.objects import fields as object_fields


@base.IronicObjectRegistry.register
class AgentChannelHead(base.IronicObject,
                       object_base.VersionedObjectDictCompat):
    """The per-node journal head that owns the agent channel."""

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
    """One agent execution process on one node."""

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
    """One frozen agent method invocation in a session."""

    # Version 1.0: Initial version
    VERSION = '1.0'

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
    }
