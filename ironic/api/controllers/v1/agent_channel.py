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

"""Agent channel work and reports endpoints.

The agent in ``agent-poll`` mode pulls work from the journal instead of
receiving conductor-initiated HTTP calls. These two routes are the public
wire surface of that transport.

* ``POST /v1/agent_channel/{node_uuid}/work`` — one bounded work poll. The
  request body carries the session UUID, the execution UUID, the poller's
  believed workflow epoch, an opaque poll ID, and a wait budget. The
  response carries the session execution epoch, its state, one outcome
  (``command``, ``no_work``, ``not_ready``, ``closed``), an optional
  command envelope, and a ``retry_after`` hint. The server clamps the
  wait budget to zero until measured capacity proves that long waits are
  safe.
* ``POST /v1/agent_channel/{node_uuid}/reports`` — one independent bounded
  list of report entries. Each entry has a command ID, a workflow epoch, a
  kind (``receipt``, ``acceptance``, ``result``), and a payload. The
  response commits one acknowledgment per entry with its disposition.

Both routes are served from the current API version (1.115); they
introduce no new microversion. They require the exact node UUID in the
path and authenticate the ``X-Ironic-Agent-Token`` header against the
session token verifiers in the journal. The verifier never
leaves the journal, and neither route touches the conductor workflow
worker: the journal is the authority and these handlers hold no task
reservation, no conductor lock, and no long transaction. The token never
appears in responses or in logs.
"""

from oslo_log import log
from oslo_utils import uuidutils
from pecan import rest

from ironic import api
from ironic.api.controllers.v1 import utils as api_utils
from ironic.api import method
from ironic.common import exception
from ironic.common.i18n import _
from ironic.db import api as dbapi

LOG = log.getLogger(__name__)

AGENT_CHANNEL_TOKEN_HEADER = 'X-Ironic-Agent-Token'


def _agent_channel_token():
    """The token presented in the agent channel header, or None.

    The token is authentication material: it is never logged and it never
    appears in any response.
    """
    return api.request.headers.get(AGENT_CHANNEL_TOKEN_HEADER)


def _check_node_uuid(node_uuid):
    """The agent channel routes take the exact node UUID; reject names."""
    if not uuidutils.is_uuid_like(node_uuid):
        raise exception.InvalidParameterValue(
            _('The agent channel routes require the exact UUID of the node, '
              'got %s.') % node_uuid)


class AgentChannelController(rest.RestController):
    """Agent channel endpoints of one node: work polls and reports."""

    _custom_actions = {
        'work': ['POST'],
        'reports': ['POST'],
    }

    @method.expose()
    @method.body('body')
    def work(self, node_uuid, body=None):
        """Answer one agent work poll against the session journal.

        The poller's believed workflow epoch fences the delivery: a stale
        poller gets ``no_work`` and the current epoch so it can
        resynchronize, and an unknown session or execution never receives
        work. A ``command`` outcome carries one envelope; every other
        outcome carries ``None`` and a ``retry_after`` hint.
        """
        if not api_utils.allow_agent_channel():
            raise exception.NotFound()
        api_utils.check_policy('baremetal:node:agent_channel:work')
        _check_node_uuid(node_uuid)
        agent_token = _agent_channel_token()
        if agent_token is None:
            raise exception.InvalidParameterValue(
                _('The %s header is required for agent channel requests.')
                % AGENT_CHANNEL_TOKEN_HEADER)
        if not isinstance(body, dict):
            raise exception.InvalidParameterValue(
                _('The agent channel work request body must be a JSON '
                  'object.'))

        session_uuid = body.get('session')
        execution_id = body.get('execution')
        if not uuidutils.is_uuid_like(session_uuid):
            raise exception.InvalidParameterValue(
                _('The agent channel work request must carry the session '
                  'UUID.'))
        if not uuidutils.is_uuid_like(execution_id):
            raise exception.InvalidParameterValue(
                _('The agent channel work request must carry the execution '
                  'UUID.'))

        # The believed workflow epoch and the wait budget must be integers.
        # The wait budget is clamped to zero: the default budget stays zero
        # until measured API, RPC, and report capacity prove that long
        # waits are safe, so the journal answers immediately either way.
        for field in ('workflow_epoch', 'wait_seconds'):
            value = body.get(field, 0)
            if not isinstance(value, int) or isinstance(value, bool):
                raise exception.InvalidParameterValue(
                    _('The agent channel work field %s must be an integer.')
                    % field)
        workflow_epoch = body.get('workflow_epoch', 0)

        db = dbapi.get_instance()
        if not db.verify_agent_channel_token(
                node_uuid, session_uuid, execution_id, agent_token):
            raise exception.AgentChannelUnauthorized()
        return db.poll_agent_channel_work(
            node_uuid, session_uuid, execution_id, workflow_epoch)

    @method.expose()
    @method.body('body')
    def reports(self, node_uuid, body=None):
        """Accept one bounded list of agent report entries.

        The reports request names only the node; the reporting session is
        identified by its token. Every stored command of a matched session
        is a valid target, so a late report of a superseded session stays
        acknowledged until a revocation unit fences the old credentials.
        The response commits one acknowledgment per entry and never echoes
        an entry's payload.
        """
        if not api_utils.allow_agent_channel():
            raise exception.NotFound()
        api_utils.check_policy('baremetal:node:agent_channel:reports')
        _check_node_uuid(node_uuid)
        agent_token = _agent_channel_token()
        if agent_token is None:
            raise exception.InvalidParameterValue(
                _('The %s header is required for agent channel requests.')
                % AGENT_CHANNEL_TOKEN_HEADER)
        if not isinstance(body, list):
            raise exception.InvalidParameterValue(
                _('The agent channel reports body must be a JSON list.'))

        db = dbapi.get_instance()
        session_uuid = db.find_agent_channel_session(node_uuid, agent_token)
        if session_uuid is None:
            raise exception.AgentChannelUnauthorized()
        acks = db.report_agent_channel(session_uuid, body)
        return {'reports': acks}
