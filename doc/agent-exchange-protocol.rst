..
 This work is licensed under a Creative Commons Attribution 3.0 Unported
 License.

 http://creativecommons.org/licenses/by/3.0/legalcode

============================================================
Agent exchange protocol (frozen implementation contract)
============================================================

Status: the contract is frozen. A change to the contract requires review
before it is applied. New B+C requirements carry the tag
``NOT_IMPLEMENTED``. Requirements that no test and no runnable implementation
prove carry the tag ``NOT_LIVE_VALIDATED``.

This contract pins the Options B and C (B+C) design decisions. It does not
replace ``doc/agent-initiated-communication.rst``. That document remains the
full assessment. This document is the short frozen contract that the
implementation must follow.

Evidence tags
=============

Use these tags exactly. They record how each statement was established.
``AUDITED_SOURCE`` marks a fact verified against the IPA checkout at the
pinned baseline ``c3feaf2``. Citations always reference that checkout. They do
NOT reference a newer sibling HEAD. ``MOCK_PROOF`` marks a controlled AST
result recorded by a source audit. The audit did NOT execute it.
``NOT_IMPLEMENTED`` marks a new B+C behavior the code does not yet have.
``NOT_LIVE_VALIDATED`` marks a decided requirement with no passing test and no
runnable implementation.

Glossary
========

Use these terms exactly as defined. ``direct-http`` is the current transport.
The conductor calls the agent HTTP API directly. ``agent-poll`` is the new
transport. The agent requests work from the Ironic API. ``session`` is one
agent execution process. A session has a stable execution UUID. A reboot
starts a new session. ``command`` is one IPA method invocation with frozen
parameters. ``invocation key`` is the stable identity of one command in a
workflow. ``journal`` is the durable database record set for sessions and
commands. ``receipt`` is proof that Ironic accepted and stored a report.
``admission`` is the point where a command becomes executable on the agent.
``result`` is the completed outcome of a command. ``retry window`` is the
period during which a lost delivery stays retrievable.

Scope
=====

The implementation preserves these behaviors. Scheduled workflow execution
stays. Heartbeats still schedule a workflow worker. The command rendezvous
stays durable. Command state lives in the database. Correlated agent replies
stay. ``AgentClient`` calls keep their synchronous request and reply shape.

The implementation adds these behaviors. The agent polls for work.
``AgentClient`` publishes a command into the journal and waits for the
correlated result. The agent submits reports to Ironic. The reports use the
command identity. The conductor recovers commands and results across restart
and takeover.

The implementation does NOT include these items.

* No broker mailbox.
* No gateway push routing.
* No streaming channel.
* No general continuation engine.
* No JSON-RPC per-call-timeout interface.

The source under test must be provable. Use the actual source working-tree
hash. A stale installed overlay is NOT proof. The end-to-end test uses a fresh
rebuild.

Baselines
=========

Ironic checkout baseline. Commit
``9c3144f6cddc9564dab2b2c2c859f8bd2aa63b5a`` (``9c3144f6c``) is the
implementation baseline at freeze time.

IPA checkout baseline. Commit ``c3feaf2`` is the implementation baseline of
the agent at freeze time. The source citations in this document reference the
IPA tree at that checkout pin.

API and RPC versions. The API maximum microversion is ``1.115`` in
``ironic/api/controllers/v1/versions.py``. The conductor RPC version is
``1.62`` in ``ironic/conductor/rpcapi.py``.

Prior art
=========

The OpenStack specification "No Conductor to IPA Communication" (review
777172) was abandoned on 2024-01-16 after the sync-heartbeat discovery. The
specification proposed to carry commands in the heartbeat response body. The
conductor answers the heartbeat RPC immediately and runs the driver heartbeat
on a separate thread, so a driver heartbeat cannot return a value.

This contract differs. Agents pull work from a separate work endpoint. The
conductor does not push commands to the agent. The heartbeat reports
capabilities only, and the asynchronous heartbeat handling stays unchanged.

Prior-art facts must be rechecked before publication. Recheck each citation,
the vote count, and the abandonment statement against the public record.

Modes and activation
====================

``direct-http`` remains the default. New installs keep full backward
compatibility. ``agent-poll`` is opt-in. The operator selects it per node or
per node configuration. The selection happens before any command producer
runs. The agent requests work only in ``agent-poll`` mode. The conductor uses
the mode for the node. The mode is stable for one session.

The agent needs a session before it can poll. If the session is absent or the
channel is not ready, the exchange returns ``channel-not-ready``. The exchange
does NOT fall back to ``direct-http``.

Optional direct fallback is allowed ONLY when all conditions hold. The
fallback happens before any publication. A configured agent listener is
available for the node. The configured policy permits the fallback. If any
condition is false, the exchange returns ``channel-not-ready``.

Backend selection
=================

The transport backend selection covers all construction sites of
``AgentClient``. The sites are ``agent_client.py:get_client(task)``,
``agent_power.py:AgentPower`` which constructs ``AgentClient()`` directly, and
``drivers/utils.py:collect_ramdisk_logs`` which constructs ``AgentClient()``
from a node only.

All client methods route through the stable mode and session for the node.
Unsupported methods reject BEFORE publication. The rejection uses a distinct
transport exception. This exception must NOT trigger the old IPA service
fallback by accident.

The conductor never replays an unknown or destroyed command automatically.
This applies after reboot, restart, and takeover. Conductor restart and
takeover keep these outcomes. There is no lost-reply automatic replay.
Deduplication works. An unknown or destroyed destructive command NEVER
auto-replays.

Endpoints and versions
======================

The parent selected these endpoint names.

* ``POST /v1/agent_channel/{node_uuid}/work``
* ``POST /v1/agent_channel/{node_uuid}/reports``

The heartbeat extension adds capability fields. The heartbeat reports the poll
mode, the protocol version, and the supported method profile.

The new API feature uses the next microversion after the actual maxima at
implementation creation time. The API maximum is ``1.115`` at the pinned
baseline. The new microversion becomes ``1.116`` when implementation is
created. The new conductor RPC methods use the next RPC version after
``1.62``. The implementation adds the separate release-mapping cases for the
new RPC version. No JSON-RPC per-call-timeout interface is required.

Security and policy
===================

The agent token authenticates the public routes. The routes require an exact
node UUID. The routes do NOT bypass or weaken Keystone policy.

TLS policy. The sandbox lab may use HTTP. Production and shared infrastructure
REQUIRE TLS. Proxy request budgets are measured. Agent tokens never appear in
logs. Agent tokens never appear in URLs. The token header is
``X-Ironic-Agent-Token``. This header authenticates the agent channel.

Report authentication is independent of work authentication. The session token
verifier compares a SHA256 of a random secret with a constant-time compare.
The API never returns the established token.

Current lookup behavior stays. With a known IPA token, lookup masks the new
value as ``******`` and keeps the known token. Without a known IPA token,
lookup locks down. The current inspection request retains the UUID only. It
adds no new microversion body field. The implementation does NOT weaken the
existing bootstrap security.

Revocation
==========

Security revocation is explicit and absolute. These events revoke the node: a
Node object DELETE, an administrative agent-token revocation, and a security
abort.

Revocation invalidates ALL retained session and token verifiers for the node.
Revocation FENCES queued work. After revocation there is no new delivery, no
activation, and no workflow advance from the old authority. The old report
credentials become unusable. They CANNOT issue a late acknowledgment and they
CANNOT submit a duplicate report. Revocation TRUMPS retention and retry. No
TTL and no retry window overrides a revocation.

Ordinary workflow token cleanup is different. When the workflow finishes or a
token cycles WITHOUT a security event, the implementation MAY keep a
REPORT-ONLY verifier for stored commands within the TTL. This verifier
acknowledges a stored report by its stored command ID only. It cannot create
work, activate a command, or advance a workflow. The default cleanup must NOT
ambiguously bypass security actions. Security revocation is the default
authority over report retention. The code must make the cleanup distinction
explicit and reasoned. The tests must cover both paths.

A node-workflow recycle is NOT the same as a Node object DELETE. A recycle
deletes the target and creates a new workflow. Revocation still applies to the
old identity. This distinction is explicit. Tests are REQUIRED for future
code. They are NOT proven here.

* After revocation, the old token cannot submit a report.
* After revocation, the old token cannot acknowledge a report.
* After revocation, the old token cannot create work.
* After revocation, the old token cannot activate a command.
* Unknown or destructive outcomes reconcile without replay.

Revocation does NOT delete journal records. Revocation does NOT disable the
feature. Archival retention protects unresolved audit data. Authorization
revocation does NOT mean erasing the audit record. A bounded historic report
stays valid by its stored command ID only. It does NOT advance the current
workflow. The token verifier keeps the secure constant-time compare. TLS,
header transport, and no-token-in-logs requirements are unchanged. A future
integration may add an explicit OpenAPI token-reset action. This contract
records it as preflight. It is ``NOT_IMPLEMENTED``.

Bootstrap and heartbeat
=======================

The initial heartbeat capability report carries the mode, protocol version
``1``, and one execution UUID that is stable per process. The report also
advertises the supported method profile. ``NOT_IMPLEMENTED``

The conductor authenticates the current token first. It persists the
agent-to-session assignment with retry consistency. Only then does it spawn
the workflow executor. ``NOT_IMPLEMENTED``

The IPA heartbeat must consume the JSON success body. The baseline IPA ignores
the body and returns ``None``. The version cap is at
``ironic_python_agent/ironic_api_client.py`` lines 37-40.
``MAX_KNOWN_VERSION`` caps the announced API version. The ``heartbeat``
method at ``ironic_python_agent/ironic_api_client.py`` lines 165-200 posts
the heartbeat and does not parse the success body. ``AUDITED_SOURCE``. The
required consume-the-body change is a B+C behavior. ``NOT_IMPLEMENTED``

The current ramdisk heartbeat allows an empty callback URL. See
``ramdisk.py`` lines 269-284. It blocks a changed non-empty ``agent_url``. See
``ramdisk.py`` lines 285-294. Activation must validate and persist the
transport at the first heartbeat. This happens before the first command
producer. Activation must NOT erase the URL of an arbitrary existing direct
session. ``NOT_IMPLEMENTED``

Listener-free IPA mode skips ``Application.start`` and automatic listener TLS.
Initialized extensions and workers still run. A separate heartbeater and a
separate reporter run in this mode. The baseline ``serve_ipa_api`` starts the
listener before the heartbeater. See ``ironic_python_agent/agent.py`` lines
486-498. ``AUDITED_SOURCE``. The listener-free change is opt-in only.
``NOT_IMPLEMENTED``

Locking and work wait
=====================

The work wait holds NO Node task reservation and NO database transaction. The
correlated synchronous ``AgentClient`` wait MAY retain the existing exclusive
Node task reservation. It holds NO database transaction. The ordinary command
producer CAN retain the exclusive reservation while it waits. It holds no
database transaction during the wait.

The report and work dispatcher does NOT acquire a ``TaskManager`` reservation.
It does NOT block on a Node-row update or Node-row lock. It is independent of
the heartbeat workflow worker and of the workflow capacity.

The durable journal is the authority. Journal queries give wake hints only.
Lost notifications are safe. Restart is safe. The existing reservation is a
committed Node field. It is not a long database transaction. See
``ironic/objects/node.py`` field ``reservation``.

The runtime acceptance test requires this outcome. A reply commits and wakes a
caller while the caller holds an exclusive ``Reserved`` state. This test must
pass. The implementation does NOT add a blanket global unlock. Selective
explicit lock-release phases are possible only later. Each phase needs a
fence. Each phase needs proof that it is necessary.

``wait_seconds`` is a receiver-budget argument. The server clamps the value to
a monotonic zero or bounded value. Short poll and long poll use the same
protocol and the same implementation boundary. The default budget is zero. It
stays zero until measured API, RPC, and report capacity prove that long waits
are safe. The wait uses a monotonic deadline. Repeated wake-ups do not extend
the deadline. A normal expiry returns a successful no-work response. The
response includes the acknowledgments for accepted reports.

Journal
========

The journal uses three tables in the existing SQLAlchemy database. The tables
are ``head``, ``session``, and ``command``. Records are versioned objects. The
journal is durable. The Node has NO JSON list that is the authoritative
command list. Open requirements for this model keep the constraints already
reviewed.

``head`` fields. Unique Node foreign key. Current workflow epoch. Current
session. Owner conductor process incarnation and fencing generation. One
outstanding effect slot and scheduling claim.

``session`` fields. UUID unique per ``(node, execution_id)``. Immutable
protocol profile. Token verifier reference. Lifecycle and liveness closure.

``command`` fields. UUID stable per ``(session, workflow_epoch,
invocation_key)``. Unique sequence. Immutable command parameters. The
immutable part is the parameter fingerprint and boundary. Receipt, acceptance,
and result report slots. Digests, reconcile state, and consumption state.

Tests use portable conditional updates. The unique constraints must work on
SQLite, MySQL, and PostgreSQL.

The invocation key is an explicit step-occurrence or method slot. It is NOT a
mere command name. Repeated steps create distinct commands. A retry reuses the
command ID with frozen parameters. A changed body for the same ID is a
conflict.

Reports stay valid for stored historical commands. The API can acknowledge
them after the workflow advances and after the token and Node are cleaned up.
Old credentials NEVER activate a command or deliver work again. The journal
retains unresolved commands and tombstones. It advertises their retry window.
Storage backpressure bounds retention. The journal never acknowledges a
command and then discards it.

All publication, reply, and consumption operations use short transactions.
Reply ingestion makes NO Node update and NO workflow advance. Workflow command
consumption advances the Node phase and marks the journal consumed atomically.
This happens under the existing normal task lock and fence. It is NOT a
sequence of independent object saves.

Activating a session for a node atomically supersedes that node's previous
session. The superseded session keeps its journal rows. The fence of a
superseded session is invalid for publication. Publishing into a superseded
session is rejected with a distinct ``AgentChannelFenceConflict`` fence
conflict error.

A stale producer, session, or epoch receives no new work, no consume, and no
duplicate advance. In this rule, "no new work" includes journal publication,
not only work delivery. A late valid result from a superseded session MAY be
acknowledged. It receives no new work authority.

A waiter whose session is superseded while it waits receives no result for
that wait. The wait ends no later than the existing ``command_timeout``.
Recovery is to re-issue the command into the current session under its own
invocation key. A retry publication into the superseded session fails with
``AgentChannelFenceConflict``.

Wire protocol
=============

Work request. The agent sends session, execution, poll ID, and
``wait_seconds``. A ``200`` response carries the session execution epoch and
its state. The states are ``active``, ``draining``, ``closing``, and
``closed``. A superseded session reads state ``draining`` and its work poll
returns ``no_work``. It MAY still submit reports and late results until it
closes or its timeout elapses.

The response carries one outcome. The outcomes are ``command``, ``no_work``,
``not_ready``, and ``closed``. The ``command`` outcome carries one envelope.
The other outcomes carry ``null`` and a ``retry_after`` hint.

Envelope fields. Stable command UUID. Workflow and execution ID. Command
sequence. Command name. Parameters. Command boundary. Acceptance and
completion states. Terminal admission state.

Reports. The agent submits an independent bounded list. Each entry has command
ID, epoch, and kind. The kinds are ``receipt``, ``acceptance``, ``result``,
and ``terminal_closed``. Each entry has a payload. The reports response
commits acknowledgments for each entry. The response reports the kind and
disposition of each acknowledgment.

IPA keeps reports until the acknowledgment is durable. Reports are
independent of the work request and of command execution. A lost delivery
response stays retrievable until it is accepted durably. These states are
distinct. ``receipt``. IPA ``admission``. ``result``. A no-work response NEVER
returns an empty command-history list. This protects fresh-boot and
empty-history cases.

Status journal and result conversion
====================================

The status journal is local to one session. It stores the ordered actual
envelopes. The stored names are UNQUALIFIED. An accepted IPA command name is
``command_name`` with no extension prefix.

A status entry has these fields. Accepted IPA ID. ``command_name``.
``status``. ``error``. ``result``. A status entry has NO ``params`` field. The
actual result may be ``None``.

Conversion rules. A synchronous result converts to the string
``{'result': v}``. An asynchronous result converts to the string
``{'result': 'cmd: v'}``. A list of tuples converts per the documented tuple
order.

Services fallback. The services path falls back to a raw dictionary only in
the known existing fallback case. That case is a definitive IPA unsupported
response. There is NO transport fallback for a feature that is unsupported.

AgentClient method boundaries
=============================

Asynchronous commands return the actual IPA acceptance. The methods are
``execute_clean_step``, ``execute_deploy_step``, and ``execute_service_step``.

Synchronous commands return the completed envelope. The methods are
``get_clean_steps``, ``get_deploy_steps``, ``get_service_steps``,
``get_partition_uuids``, ``sync``, ``collect_system_logs``, and rescue
finalization. ``install_bootloader`` returns acceptance and then polls to
completion.

``finalize_rescue``. The classification follows the IPA sync decorator. The
baseline ``rescue.finalize_rescue`` uses ``sync_command``. It returns a
completed ``SyncCommandResult``. It sets ``serve_api`` to false before the
result is returned. See ``ironic_python_agent/extensions/rescue.py`` lines
56-69. ``AUDITED_SOURCE``. Ironic's public method sends the HTTP request with
its default wait flag (``wait=False``). The completion boundary comes from the
IPA sync decorator, NOT from the Ironic HTTP flag. Classify ``finalize_rescue``
as completed and synchronous regardless of the HTTP flag.

The baseline IPA raises ``errors.AgentIsBusy`` when a command arrives while
the previous command still runs. See
``ironic_python_agent/extensions/base.py`` line 264. ``AUDITED_SOURCE``. This
serializes effects. There is NO implicit pending queue in the baseline. The
baseline IPA has an ordered history and a busy lock.

Atomic admission. In ``agent-poll`` mode, admission binds the new stable ID
BEFORE an asynchronous start. This is a B+C behavior. ``NOT_IMPLEMENTED``. The
baseline IPA async path starts execution before the command history insertion.
See ``ironic_python_agent/extensions/base.py`` lines 250-285 and 288-318.
``AUDITED_SOURCE``.

A duplicate ID with the same parameters does NOT re-execute. A changed body
for the same ID is rejected. Retention is bounded within one execution. A
reboot fences unknown outcomes.

Reporter snapshots must NOT need the ``command_lock`` or the hardware manager.
A synchronous command holds that lock. The distinct ``command_state_lock`` can
serialize history while execution is blocked. The source audit recorded
controlled AST results for this. ``MOCK_PROOF``. The B+C use of that lock is
``NOT_IMPLEMENTED``.

Terminal barriers
=================

The parent decision sets minimum barriers. There is NO new mandatory
``terminal_release`` protocol.

Public terminal commands. ``power_off``, ``reboot``, and ``lockdown`` are
covered here. They are terminal-admission and terminal-barrier commands.

The baseline standby terminal commands are asynchronous. They run
``full_sync`` (syncfs) and then reboot or power off. They fall back to
sysrq-trigger on failure. The result stays ``RUNNING``. There is no delay
gate. See ``ironic_python_agent/extensions/standby.py`` lines 1064-1117.
``_run_shutdown_command`` runs the shutdown or reboot command with the sync
and sysrq fallback. ``AUDITED_SOURCE``.

In ``agent-poll`` mode, terminal commands follow this order. Admit and record
the report acceptance. Wait for the durable acceptance ACK. Start the terminal
side-effect worker only after the ACK. ``NOT_IMPLEMENTED``

A server that sees only acceptance returns the existing asynchronous envelope.
Terminal success requires INDEPENDENT evidence. Use management power-off,
management reboot, or OS boot. Heartbeat absence is NOT success. A lost ACK
retains the command ID. The reporter retries it. There is no execution until
the ACK is confirmed.

Independent installed root evidence. Terminal success after an OS boot
requires actual root-device and filesystem evidence on the target volume. A
banner alone is INSUFFICIENT. Reject a RAM or overlay fallback. Use fresh
serial root-mount evidence or a bounded read-only probe. Do NOT require an
unrestricted guest shell. OS boot requires a fresh console. The final power-off
must match the database hardware record.

Rescue and system lockdown. The baseline synchronous path sets ``serve_api``
to false and signals lockdown termination before the result wrapper. See
``extensions/rescue.py`` lines 56-69 and ``extensions/system.py`` lines 25-29.
``AUDITED_SOURCE``. In ``poll`` mode, keep a restricted report-only lifecycle.
Keep it until the actual result is durably acknowledged or until a bounded
teardown deadline. Then honor the termination. ``NOT_IMPLEMENTED``

The implementation does NOT report success on silent network disappearance.
These states are distinct. Command admitted. Result available. Effect
observed.

The final deploy reboot must NOT require another IPA boot. The final reboot
uses management power and the fresh installed OS console. Generic standalone
agent power has no independent evidence. It gets an explicit unsupported or
unknown profile. Closing a session admits no new work. Report-only historical
acknowledgment is bounded. Closing never restores admission.

Capacity
========

The reports path needs real budgets. The budgets cover WSGI, RPC, database,
and report pool resources. The budgets are SEPARATE from the work-wait slots.
The default budget is zero until measurements decide the values. Backpressure
policy. ``429`` for backpressure. ``503`` for unavailable. Read-only API,
artifacts, and heartbeats stay functional under load on both RPC backends.
``NOT_LIVE_VALIDATED``

The serving process admits at most ONE work waiter per session. A duplicate
poll gets no extra admission. At most ONE outstanding effect command exists.
Command status may overlap locally. A second effect command is NOT admitted.

Timeouts. ``agent.command_timeout`` has a current default of 60 seconds in
``ironic/conf/agent.py`` lines 115-116. This document asserts NO outer-timeout
numeric as a configuration value and NO B+C production default. The budgets
must be measured and recorded. The full delivery path must complete before
every timeout in the chain. The path includes queueing, request processing,
response handling, the RPC transport, and the IPA HTTP and proxy transport.
The whole budget is measured with margin. No JSON-RPC per-call-timeout
interface is required. A planned outer harness deadline is an observation
only. It is NOT a configuration value and NOT a validated B+C budget. This
feature is not implemented. Liveness and capacity are not proven.
``NOT_LIVE_VALIDATED``

Upgrade and drain
=================

All conductors that own a node group must support the RPC, object, and schema
versions. Only then is activation eligible. During drain, new activation is
restricted. Existing pinned-mode sessions continue. Reports are always
accepted for stored commands. ``NOT_IMPLEMENTED``

The feature does NOT flip from direct to poll immediately. An unresolved
downgrade is not allowed. Administrative inspect and drain CLI and API are
future items. They need tests. Operators MAY disable new activation and drain
existing pinned sessions. They must NOT delete journals. They must NOT
silently flip pinned sessions to direct mode. An immediate security abort
revokes authority and records unknown outcomes. It does NOT prove the
cancellation of a running disk operation.

Acceptance
==========

Acceptance of the protocol itself is defined by the whole-feature
requirement-to-check matrix owned by the implementing plan. This document
defines the contract only. Lab acceptance does not appear in this portable
document. The implementing plan keeps that matrix with its required checks and
evidence.

Validation order
================

Implement the tests in this bounded order. Each label is a CONTRACT, not a
passing test.

#. Journal identity.
#. Atomic admission.
#. Reports, fence, and rendezvous.
#. Exclusive-reservation wake and backend boundaries.
#. Activation API and profile.
#. IPA admission, report, and lifecycle.
#. Terminal barriers.
#. Revocation, including positive and negative cases.
#. Recovery, capacity, and compatibility.

The revocation tests are REQUIRED for future code. They are not proven here.
Cross-process notification is an explicit risk. The durable poll truth does
NOT need a shared broker.

Final verifier and change control
=================================

This contract is frozen. A change to the contract requires review before it is
applied.

A final whole-feature verifier is required. The verifier runs all acceptance
outcomes that the implementing plan defines. The verifier is a document. A
passing verifier document does NOT prove that B+C behavior works. The behavior
is proved only by a runnable implementation and its tests. The verifier does
not weaken a frozen requirement.

The original assessment ``doc/agent-initiated-communication.rst`` is preserved
unchanged. It remains the authoritative assessment.

Source references
=================

These references are current on the pinned baselines.

Ironic references:

* ``ironic/api/controllers/v1/ramdisk.py``. Empty-callback allowance at lines
  269-284. Agent URL change rejection at lines 285-294.
* ``ironic/api/controllers/v1/versions.py``. Microversion list. Maximum is
  ``1.115`` at the pinned baseline.
* ``ironic/conductor/rpcapi.py``. RPC version ``1.62``.
* ``ironic/conductor/manager.py``. Heartbeat validation, worker scheduling,
  token validation, ``get_node_with_token``.
* ``ironic/conductor/utils.py``. ``is_agent_token_valid``,
  ``add_secret_token``.
* ``ironic/conductor/task_manager.py``. Shared and exclusive locks.
* ``ironic/objects/node.py``. ``reservation`` field.
* ``ironic/drivers/modules/agent_client.py``. ``get_client``,
  ``AgentClient``, ``_command``, ``get_commands_status``.
* ``ironic/drivers/modules/agent_base.py``. ``HeartbeatMixin.heartbeat`` and
  ``agent_url`` persistence.
* ``ironic/drivers/modules/agent_power.py``. ``AgentPower`` direct client
  construction and reboot handling.
* ``ironic/drivers/utils.py``. ``collect_ramdisk_logs`` node-only client
  construction.
* ``ironic/conf/agent.py``. ``command_timeout`` default at lines 115-116.

IPA references:

* ``ironic_python_agent/agent.py`` lines 486-498. ``serve_ipa_api`` starts
  the listener before the heartbeater.
* ``ironic_python_agent/api/app.py`` lines 131-156. ``Application.start``
  runs the WSGI server and optional TLS.
* ``ironic_python_agent/ironic_api_client.py`` lines 37-40.
  ``MAX_KNOWN_VERSION`` caps the announced API version.
* ``ironic_python_agent/ironic_api_client.py`` lines 165-200. ``heartbeat``
  posts the heartbeat and does not parse the success body.
* ``ironic_python_agent/extensions/base.py`` line 264. ``errors.AgentIsBusy``.
  Lines 321-348. ``sync_command``.
* ``ironic_python_agent/extensions/standby.py`` lines 1064-1117.
  ``_run_shutdown_command`` runs the shutdown or reboot command with syncfs
  and sysrq fallback.
* ``ironic_python_agent/extensions/rescue.py`` lines 56-69.
  ``finalize_rescue`` sync command and ``serve_api`` disabling.
* ``ironic_python_agent/extensions/system.py`` lines 25-29. ``lockdown`` sync
  command and ``serve_api`` disabling.
