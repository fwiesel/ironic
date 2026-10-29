..
 This work is licensed under a Creative Commons Attribution 3.0 Unported
 License.

 http://creativecommons.org/licenses/by/3.0/legalcode

============================================================
Agent-initiated command communication
============================================================

Status: design discussion draft; no implementation or API contract is approved.

This document assesses how ironic-python-agent (IPA) can receive commands
over connections initiated by IPA. It compares technical feasibility,
implementation effort, resource costs, and recovery behavior, and recommends
a limited implementation path rather than prototyping every alternative.

The implementation baseline is Ironic master at commit
``9c3144f6c`` (22 October 2026). IPA observations refer to revision
``c3feaf2`` (18 November 2025). A pinned, current IPA revision still needs to
be audited before implementation; the IPA observations below do not establish
current master behavior.

Endpoint names, configuration options, message fields, and interfaces proposed
below are illustrative. They do not describe existing supported configuration.

Problem description
===================

Current agent-based deployment requires connections in both directions:

* IPA contacts the Ironic API for lookup and heartbeats.
* The conductor contacts IPA's advertised API to submit commands and retrieve
  results.

Some installations permit managed nodes to reach the Ironic API but do not
permit conductors to initiate connections to those nodes. The requirement is
to support those installations while retaining existing direct communication
for deployments that use it.

The network constraint is independent of whether the installation uses a
dedicated provisioning network, routed rack networks, network address
translation, or another topology. The essential constraint is that the
conductor cannot reach the agent's listening endpoint.

The externally reachable service should remain the Ironic API. Internal RPC,
database, and optional message-broker connections remain private. An additional
API backend behind the same public address is an option, not a requirement.

Goals
-----

* Deliver agent commands and collect results over IPA-initiated connections.
* Preserve periodic liveness reporting and prompt reporting after commands
  complete.
* Support a controlled upgrade of Ironic and IPA at different times.
* Define behavior for empty responses, retries, lost responses, and restarts.
* Reuse existing command names, parameters, and result representations where
  practical.
* Evaluate a bounded HTTP long poll with a corresponding blocking conductor
  RPC, without assuming that the whole stack must become asynchronous.

The scope includes deployment and cleaning, and must explicitly account for
servicing, rescue, capability discovery, and other direct IPA calls. Providing
general-purpose remote execution in deployed tenant operating systems is not
the goal.

Network and protocol boundaries
-------------------------------

IPA needs a reachable Ironic API endpoint, not a public address of its own.
Command transport and artifact access remain separate. Where IPA can reach
the public Ironic API and Swift artifact endpoints, an agent-initiated command
channel can use that connectivity without carrying images or changing the
artifact download path.

Two changes can be implemented independently: IPA can proactively report
capabilities and results, and Ironic can deliver commands through IPA-initiated
connections. Proactive reporting can reduce direct calls even in deployments
that retain bidirectional connectivity.

Use explicit mode names such as ``direct-http`` and ``agent-poll``. The latter
means that IPA requests work from Ironic. A reverse tunnel also uses an
IPA-initiated connection, but retains conductor-originated HTTP requests and
a local IPA listener.

Recommended direction
=====================

Start with scheduled workflows, a command rendezvous, and correlated replies
that preserve existing ``AgentClient`` return behavior (Options B and C).
Use an HTTP work exchange with a receiver-enforced wait budget. Zero wait and
bounded long polling use the same protocol and implementation boundary.

This combination preserves workflow call sites and both existing remote RPC
backends. The outstanding RPC supplies the response route to the API process
holding the agent request. It needs neither reverse gateway routing nor a
mandatory broker, and does not require asynchronous API hosting.

The principal feasibility gate is independent delivery and reply ingestion
while a workflow caller holds a node lock. The principal scale limit is the
number of waiting API requests, RPC handlers, and workflow callers. A software
wait timeout bounds each wait; it does not eliminate those resource costs.

Recommended implementation boundaries:

* Prefer a dedicated, microversioned work-exchange endpoint. Existing heartbeat
  processing assumes a callback URL and schedules workflow work; report
  ingestion needs a path independent of that workflow's lock. Extending
  heartbeat is possible if it provides the same separation.
* Preserve acceptance responses for asynchronous operations and completed
  results for synchronous operations. Retain status retrieval initially where
  this avoids changing workflow semantics.
* Provide prompt report submission while a work poll is outstanding, using
  an independent request path or independently executable calls to the same
  endpoint. Endpoint naming alone does not provide execution independence.
* Prefer database-backed command and accepted-result records when recovery
  across conductor restarts is required. A command journal can support
  workflow-specific reconciliation without a general continuation engine.
* Start with one outstanding side-effecting command per agent execution
  session. Specify separately how status queries and control messages can
  overlap it. Add pipelining only if measurements justify the added ordering
  and recovery rules.
* Convert individual synchronous sequences to continuations only when their
  lock duration or recovery requirements justify it.

A reverse tunnel is the conditional alternative if a local IPA listener and
existing lost-response behavior are acceptable, especially if a suitable relay
can be reused. Building a new highly available relay can consume much of its
workflow-compatibility advantage.

Defer gateway push routing, broker mailboxes, gRPC, and agent-executed plans.
Introduce streaming or asynchronous hosting only when measured connection
capacity, request volume, or event frequency warrants it. The alternatives
below identify when each would become useful; they do not all need prototypes.

Recovery scope to select before implementation
------------------------------------------------------------

Connectivity, duplicate-safe delivery, and automatic workflow recovery are
different guarantees. A minimal tunnel can preserve existing failure behavior.
A native exchange should prevent duplicate execution within an IPA session,
acknowledge reports reliably, and identify obsolete or unknown-outcome work.
Automatic continuation after every restart is a stronger requirement.

For the initial native implementation, the recommended target is:

* Retries within an execution session retain command identity and do not start
  the same command twice.
* Commands and acknowledged results survive conductor restart, with enough
  workflow association to reconcile them.
* If the outcome cannot be established, the workflow stops with an explicit
  reconciliation requirement instead of replaying a destructive command.
* IPA reboot creates a new execution boundary. Automatic replay of destructive
  commands from the previous boot is unsupported.

These are proposed guarantees, not an approved contract. Safe recovery can
include an explicit failure requiring intervention; it need not mean seamless
automatic resumption. The selected guarantee must be held constant when
comparing implementation effort.

Current architecture
====================

Communication paths
-------------------

The current sequence is approximately::

  IPA                 Ironic API                 Conductor
   |                       |                         |
   |--- lookup ----------->|--- RPC as needed ------>|
   |<-- identity/config ---|                         |
   |                       |                         |
   |--- heartbeat -------->|--- heartbeat RPC ------>|
   |<-- accepted ----------|                  schedule worker
   |                       |                         |
   |<---------------- direct command POST -----------|
   |----------------- command response ------------->|
   |                       |                         |
   |--- completion HB ---->|--- heartbeat RPC ------>|
   |<---------------- direct status GET -------------|
   |----------------- command results -------------->|

The heartbeat advertises the callback URL and updates
``agent_last_heartbeat`` when processed. Command results are retrieved
separately. Deployment, cleaning, rescue, and servicing heartbeat paths may
advance the workflow; some states only record liveness.

The heartbeat RPC schedules a workflow worker. That worker's return value does
not flow back through the original RPC and HTTP response. A command exchange
therefore needs either inline command production or a rendezvous with the
scheduled worker.

The manager acquires a shared task lock for heartbeat admission. The workflow
worker then attempts to upgrade to an exclusive lock before recording liveness
and processing the workflow. Results for a caller waiting under an exclusive
lock cannot depend on this same workflow-processing path.

The relevant integration points are:

* ``ironic/api/controllers/v1/ramdisk.py``: lookup, heartbeat, and integrated
  inspection endpoints.
* ``ironic/conductor/rpcapi.py``: versioned conductor RPC abstraction, including
  oslo.messaging, JSON-RPC, and local transport selection.
* ``ironic/conductor/manager.py``: heartbeat validation and worker scheduling.
* ``ironic/drivers/modules/agent_base.py``: heartbeat-driven workflow handling.
* ``ironic/drivers/modules/agent_client.py``: direct command submission and
  result retrieval. ``get_client(task)`` currently constructs the REST client.

Keep-alive distinctions
--------------------------------------------------------------------------

The conductor's own keep-alive updates its database service record. It is
separate from IPA traffic. Its interval and inactivity defaults remain 10 and
60 seconds; SQLite deployments skip that keep-alive loop.

The API's ``ramdisk_heartbeat_timeout`` remains 300 seconds. The inspected IPA
checkout uses normal intervals randomized to 30--60 percent of that value,
or 90--180 seconds, with shorter retries after failures. Asynchronous command
completion requests a forced heartbeat, subject to minimum spacing.

Provisioning callback timeouts and fast-track freshness checks are separate
from the heartbeat cadence. A new work-poll timeout must not implicitly replace
all of these mechanisms.

Execution and request capacity
------------------------------

Master uses native threads and ``futurist.DynamicThreadPoolExecutor``.
The default conductor worker limit is 300, with 5 percent reserved. Agent
heartbeat workers explicitly cannot use the reserved pool.

The built-in REST and JSON-RPC listeners use Cheroot. The API application is
still Pecan/WSGI. Existing custom API middleware extensions are WSGI extensions,
not native ASGI routes.

The built-in WSGI service does not supply a Cheroot thread count. The inspected
Cheroot dependency starts with ten request threads and does not automatically
grow the pool when requests are enqueued. This applies to the built-in API and
JSON-RPC listeners, not necessarily to externally hosted WSGI deployments.
Request-pool configuration and admission limits are implementation concerns
for long polling; the conductor workflow pool does not provide this capacity.

Consequently, bounded blocking long polling is possible today, but consumes
waiting execution capacity. Eventlet removal does not make existing RPC calls
or WSGI requests awaitable.

An asynchronous frontend can reduce connection-waiting costs but does not make
blocking internal RPC or workflow calls asynchronous. Each execution boundary
needs its own capacity and cancellation behavior.

Proposed change
===============

The recommended starting point uses a common agent command-exchange contract.
The following alternatives assess how transport and execution boundaries can
change without conflating their costs. The recommendation remains subject to
the lock-independence, recovery, and capacity feasibility tests.

Four decisions should be made separately:

#. How does IPA obtain commands: short polling, long polling, a stream, or a
   tunnel?
#. How does conductor workflow execution hand commands to that transport:
   inline processing, preserved synchronous request/reply calls, or
   result-driven continuations?
#. Where is authoritative command state held: in a process, in the database,
   or in a broker with workflow-state reconciliation?
#. How does a gateway obtain work: internal polling, routed push delivery, or
   notifications followed by retrieval?

A worker-to-request rendezvous can connect command production to delivery with
either blocked callers or result-driven continuations. These choices are
composable rather than mutually exclusive.

This assessment covers the principal architecture families that retain the
Ironic API as the agent-facing entry point. Other protocols and infrastructure
products may implement these families without introducing a materially
different design. Direct agent access to a message broker conflicts with this
boundary. VPNs and SSH tunnels belong to the tunnel/relay family; other HTTP
streaming formats belong to the streaming family.

Long polling does not inherently require a persistent broker mailbox, an ASGI
server, or a rewrite of the conductor around asyncio.

Prerequisites and blockers below are conditions for a particular combination
of options. They distinguish implementation work from a combination that cannot
meet its selected requirements. Recovery guarantees must be held constant when
comparing complete designs.

Command exchange contract
-------------------------

An exchange would authenticate the agent, accept reports, and return pending
work or a defined no-work response. It may extend heartbeat under a new
microversion or use a new endpoint.

The contract should distinguish:

* Agent identity and, if introduced, execution-session identity.
* Command identity and the corresponding workflow step.
* Acceptance of a command versus completion of that command.
* A normal empty response versus an error or overloaded service.
* Capability reports versus command results versus liveness information.

If an exchange accepts reports and then waits for work, report acceptance must
commit before the work wait begins. Wait expiry does not undo accepted reports.
IPA retains reports until their acceptance is acknowledged. If the response is
lost, resubmitting a report must remain valid even after its workflow has
advanced. The server must recognize the duplicate without advancing the workflow
again. A no-work response and acknowledgment of submitted reports are separate
parts of the exchange outcome.

Command names and parameter/result structures should remain close to the
existing IPA command API. A transport retry must reuse the original command
identity; reuse of an identity with different parameters must be rejected.

Execution options
=================

Option A: inline heartbeat processing
--------------------------------------------------------------------------

Run the relevant heartbeat/workflow processing in the RPC request's execution
context and return its next command::

  HTTP request -> RPC -> workflow processing -> next command -> response

Advantages:

* Direct relationship between request processing and its response.
* No separate worker-to-request notification mechanism for commands generated
  during that call.
* Reuses existing REST and RPC infrastructure.

Disadvantages:

* Driver processing must yield a command description instead of sending it
  directly. Making the manager call synchronous alone is insufficient.
* API latency includes node-lock acquisition, driver work, and potentially
  unrelated device I/O before a command becomes available.
* Code that expects an immediate IPA result needs adaptation.
* A bounded long poll still needs a waiting mechanism if processing produces
  no command but later work can make one available.

The first investigation should establish whether a useful, limited set of
workflows can be expressed as one command-producing transition per exchange.
For sequences such as discovery, result-dependent selection, and command
submission, yielding the first command requires explicit resume state or
another execution context. This erodes the apparent simplicity of inline
processing and makes it a poor general starting point.

Prerequisites / blockers:

* Processing must return the command before requiring its result. If the
  inline workflow waits for a result while IPA waits for that same HTTP response
  to receive the command, neither can progress. Yielding the command, a separate
  delivery path, or independent execution contexts are required.
* Workflow processing needs its own bounded execution policy. A work-wait
  timeout does not bound earlier lock acquisition or unrelated device I/O.

Option B: scheduled workflow with command rendezvous
--------------------------------------------------------------------------

Retain worker scheduling. The RPC handler waits on per-node or per-session
pending-command state; the workflow worker publishes into that state::

  Waiting RPC context                  Workflow worker
  -------------------                  ---------------
  Accept reports
  Trigger processing if needed ------> Process/resume workflow
  Check pending command                ...
  Wait                                 Publish command
       <------------------------------ Notify waiter
  Read command
  Return HTTP response

The command state, not the notification, is authoritative:

* If the worker publishes first, the poll returns immediately.
* If the poll arrives first, publication wakes it.
* If no work is produced, the poll expires normally.
* If publication occurs just after expiry, the next poll retrieves the command.

Advantages:

* Preserves the current scheduling boundary.
* Separates command production from connection lifetime.
* Can support both short and long polling through one interface.

Disadvantages:

* Requires explicit synchronization and command-lifecycle management.
* Process-local state alone does not survive conductor restart or takeover.
* Repeated polls must not launch duplicate workflow workers.
* Synchronous agent calls need result correlation, cached information, or
  workflow continuations.

A condition variable with a predicate can provide a process-local rendezvous.
The check-and-wait sequence must avoid missed wake-ups. If command state is
persistent, committing the command, notifying a waiter, and rechecking state
must be coordinated; notification is only an optimization.

The RPC waiter must not hold a node lock or database transaction while waiting
for a worker that needs that same lock. Waiters must also not exhaust the
capacity needed to produce commands or accept results.

Prerequisites / blockers:

* Producers and waiters must reach the same authoritative command state. A
  process-local condition variable cannot notify another conductor process.
  Routing to one process or shared state with a notification/recheck mechanism
  is required.
* Safe lock release and independently available producer and result-handler
  capacity must be demonstrated for the selected workflow paths.

Option C: correlated command RPC preserving synchronous callers
--------------------------------------------------------------------------

Keep the driver-facing request/reply behavior of ``AgentClient``. Replace its
HTTP transport with command delivery over the agent channel and a correlated
result wait::

  AgentClient call -> publish command C -> wait for result C -> return
                            |
                 IPA poll/stream receives C
                            |
                 IPA submits correlated result C

Advantages:

* Smaller initial changes to synchronous conductor call sites.
* Existing command semantics can be preserved behind the client abstraction.
* Individual callers can later migrate to result-driven continuations.

Disadvantages:

* Retains blocked conductor threads for potentially long operations.
  Existing direct HTTP calls already block; the additional cost is delivery
  delay and time spent waiting for disconnected agents. Both can extend the
  duration for which a caller holds a node lock.
* Delivery and result ingestion must progress independently of any node lock
  held by the waiting caller.
* Timeout is not cancellation of the command executing on IPA.
* Requires correlation, late-result handling, and explicit executor capacity.

This is a compatibility strategy, not automatically an efficient long-term
execution model.

It works with short polling, long polling, or a persistent channel. Independent
work retrieval and reply ingestion are required; a persistent connection is
not. Report authentication, correlation, and waking the caller must progress
without acquiring the caller's exclusive node lock. Workflow advancement can
then use normal task locking. Reserving threads alone does not remove a lock
dependency.

A persistent bidirectional channel makes this option particularly relevant.
Connection establishment and command initiation are separate concerns: IPA
can initiate the connection while the conductor initiates commands over it.
Command production then does not have to coincide with a heartbeat response.
A response dispatcher connects the two execution contexts using command IDs.

An incremental implementation can preserve the existing distinction between
an asynchronous command's acceptance response and its eventual completion.
Existing status queries can initially travel over the channel too. Pushed
completion events can be introduced separately. This may avoid immediately
converting every driver call into a persisted workflow continuation.

The audit must inspect IPA extension behavior as well as Ironic's ``wait`` and
``poll`` flags. Capability calls use ``wait=True``; step execution normally
returns acceptance; bootloader installation uses completion polling. However,
``wait=False`` does not imply acceptance-only behavior: the inspected IPA's
``finalize_rescue`` extension executes synchronously. Its completed result
must still be returned to the conductor.

Prerequisites / blockers:

* Delivery and response dispatch must run independently of the waiting caller,
  its node lock, and its executor. Merely replacing HTTP in ``AgentClient`` does
  not establish this independence.
* Each method must preserve its actual return boundary: acceptance for an
  asynchronous command, or a completed result for a synchronous operation.

Option D: result-driven workflow continuations
--------------------------------------------------------------------------

A workflow transition records a command and the state needed to resume after
its result. The worker then releases its thread and node lock::

  Workflow transition -> record command and continuation -> release worker
                                |
                          IPA receives command
                                |
  Next transition <----- accept correlated result

Advantages:

* No workflow worker waits for agent delivery or execution.
* Persisted continuation state can support conductor restart and takeover.
* Workflow execution is independent of the agent connection lifetime.

Disadvantages:

* Synchronous call sequences must become explicit transitions.
* Command creation, result acceptance, and workflow advancement need crash-safe
  boundaries and duplicate handling.
* Recovery must distinguish a transition that has not run from one whose
  external effects have an unknown outcome.
* Persisted continuations need versioning and compatibility rules. A new
  conductor must interpret state recorded by older code; additive database
  migrations alone do not establish that compatibility.

This option can use Option B's rendezvous for delivery. The rendezvous determines
how a waiting request receives work; the continuation determines how the
workflow resumes. Existing wait states may provide suitable boundaries, but
their adequacy must be established by the command call-path audit.

This option has the largest workflow restructuring cost when applied broadly.
Prefer existing persisted wait states or small, explicit internal phases over
a generic representation of suspended call stacks. A command journal can also
support Option C with workflow-specific reconciliation after restart; durable
command storage does not require converting all callers to continuations.

Prerequisites / blockers:

* Persisted state must contain enough information to resume without the old
  worker's stack or process memory, including after a supported rolling upgrade.
* Each external side effect needs a defined reconciliation boundary before a
  failed transition can be retried.

Agent-executed batches or plans
--------------------------------------------------------------------------

Instead of issuing one command per exchange, the conductor could supply an
ordered batch or a limited execution plan. IPA would execute operations locally
and report individual outcomes and overall progress.

Advantages:

* Reduces round trips and conductor waiting for eligible sequences.
* Can continue eligible local operations during temporary communication outages.

Disadvantages:

* Moves sequencing, partial-failure handling, and cancellation into IPA.
* Gives the conductor fewer opportunities to intervene between operations.
* Adds plan-version compatibility and per-operation recovery requirements.

Prerequisites / blockers:

* The plan must be executable with IPA's local capabilities. Operations that
  require conductor-only services, device access, or policy decisions need an
  explicit return to the conductor.
* Continuing while disconnected must be permitted by the workflow's validity
  and cancellation rules.

The contract must define whether execution stops on failure, how dependent
operations obtain previous results, and what can resume after interruption.
A transport batch containing
independent command envelopes is not by itself such an execution plan.

This is a broader execution alternative that can be deferred to preserve
existing command semantics. It does not require general-purpose remote
execution or a new transport family.

Transport options
=================

Short HTTP polling
------------------

IPA submits results and requests work. The API returns promptly with a command
or no work. Conductor processing may create a command after that response.

Advantages:

* Fits Pecan/WSGI and existing synchronous RPC.
* Avoids long-lived requests and connection-owner routing.
* Simple behavior across API replicas and reconnects when state is shared or
  consistently routed to the responsible conductor.
* New requests can redistribute traffic across API replicas, simplifying
  rolling restarts without draining persistent agent connections.

Disadvantages:

* Trades command-delivery latency against request volume.
* Normal IPA heartbeat intervals are too long for responsive work retrieval.
* Needs adaptive intervals, jitter, or server retry guidance at scale.
* Delivery delay accumulates across dependent command sequences.
* Delivery delay extends worker and node-lock occupancy for preserved blocking
  callers. For commands arriving independently of a regular polling schedule
  with interval T, the average wait is approximately T/2. Immediate retrieval
  after a reply can reduce delay for subsequent commands.

Prerequisites / blockers:

* The chosen polling interval must satisfy both the delivery-latency target
  and the request budget, including retries and result submissions.

For illustration, 10,000 agents polling every five seconds produce roughly
2,000 requests per second before retries. The relevant population is running
agents, not the entire node inventory.

End-to-end blocking long polling
--------------------------------------------------------------------------

One HTTP request corresponds to one blocking conductor RPC. The conductor
returns a pending command, waits for one, or returns no work on expiry::

  IPA -> API: request work
         API -> conductor RPC: wait for work, bounded by server policy
                               ... wait ...
         API <- conductor RPC: command or no work
  IPA <- API: command or no work

A proposed server setting such as ``[agent]command_poll_max_wait`` controls
the work-wait budget. The conductor implements this timeout in software on
the receiving side of the RPC. It clamps the requested wait to server policy,
checks for pending work, and waits only if none is available. Zero means
immediate retrieval without waiting. The server may return early during
shutdown or when useful work cannot be done.

Use a monotonic deadline for the wait. Recompute the remaining budget after
every wake-up, including spurious notifications, so repeated wake-ups cannot
extend the wait indefinitely. Publication after expiry remains available to
the next exchange.

The same receiver-side implementation works with JSON-RPC, oslo.messaging,
and local calls. The wait budget is a method argument, not a request for the
RPC transport to time out. JSON-RPC's lack of a per-call client timeout option
does not block this design and does not require a client-interface extension.

Outer transport and proxy timeouts must accommodate the receiver's work wait
plus queueing, request processing, and response-handling margins. For example,
a 30-second work wait could fit within a configured 35-second RPC client
timeout and a 40-second IPA HTTP timeout if the other costs fit those margins.
These values are illustrative, not recommended defaults. Existing configured
transport timeouts can be used; per-call transport overrides are optional.

The receiver's wait deadline starts at the work-wait phase. It does not bound
time spent queued before the handler runs, earlier report processing or lock
acquisition, or unrelated device I/O. Those costs need admission limits and
their own execution policies. No node lock or database transaction should be
held through the work wait.

Normal software wait expiry returns a successful no-work response, including
acknowledgments for accepted reports. A transport timeout or failure is a
different outcome and may be mapped to a retryable API error. It neither undoes
accepted reports nor proves that receiver work was cancelled. The receiver
still exits its bounded work wait if the RPC or HTTP caller disconnects.

Advantages:

* Directly reuses the existing conductor RPC abstraction.
* No intermediate RPC polling or per-agent RabbitMQ queues are necessary.
* Prompt delivery once work becomes available.
* One protocol can cover short and long polling.

Disadvantages:

* A waiting WSGI request occupies a request worker, and the conductor RPC
  handler occupies execution capacity. JSON-RPC can also occupy a server
  request worker at the conductor.
* Limits and executor isolation are necessary to prevent waiter starvation of
  producers and result handlers.
* HTTP disconnect does not automatically propagate RPC cancellation.
* RPC transport behavior and timeout handling need testing for each supported
  transport rather than assuming identical resource usage.

This is a valid initial design at bounded concurrency. Asyncio can reduce
waiting costs later; it is not required for the protocol.

Prerequisites / blockers:

* Every constrained layer must support the target number of waiters while
  retaining capacity for ordinary API traffic, producers, and result handlers.
  API request workers, RPC handlers, connection pools, and workflow workers
  must be sized separately rather than treated as one conductor pool.
* The deployed proxy and server timeout limits must allow the selected wait
  budget and response-handling margins.

For illustration, 10,000 continuously polling idle agents with a 30-second wait
keep approximately 10,000 external requests outstanding, even though expiry
causes only roughly 333 new requests per second. This assumes negligible gaps
between polls and no early responses. Low request churn does not imply low
concurrency.

Reducing the wait duration does not substantially reduce outstanding requests
when agents immediately poll again. It increases request churn while retaining
approximately one waiter per agent. Limit admitted waiters and preserve
capacity for reports and ordinary traffic; timeouts alone do not prevent
starvation.

An important advantage is that the outstanding RPC already identifies the
return path to the API process holding the agent's HTTP request::

  IPA -> API process P -> blocking conductor RPC
  IPA <- API process P <- command in RPC response

This holds for both JSON-RPC and oslo.messaging-backed RPC. No registry of
agent connections on API processes is required for response routing. A
scheduled workflow still needs a rendezvous with the waiting conductor RPC
handler, but there is no separate conductor-to-gateway callback to route.

Async API long polling with short internal RPC calls
--------------------------------------------------------------------------

An ASGI endpoint holds the external request and periodically checks for pending
commands through short RPC calls. It uses asynchronous delays between checks.

Advantages:

* Does not hold one conductor request context for the entire agent wait.
* Preserves the existing RPC transport choices.
* Does not require broker notifications or cross-replica connection routing.
* External connection capacity can scale independently of conductor execution
  capacity.

Disadvantages:

* Moves polling into the API rather than eliminating it.
* Internal check frequency determines delivery latency and RPC load.
* Requires an ASGI hosting path. Blocking RPC calls need bounded thread
  offloading or an asynchronous client implementation.
* Cancelling the external request does not necessarily stop an offloaded
  blocking RPC; its thread can remain occupied until the RPC ends.

Prerequisites / blockers:

* The check interval and offload limits must meet the internal RPC load and
  delivery-latency targets, including during conductor outages.
* The ASGI route must integrate the API controls described under REST API
  impact.

This is an alternative to the end-to-end blocking design, not a prerequisite
for implementing long polling.

SSE or WebSocket command channel
--------------------------------------------------------------------------

IPA establishes a persistent outbound connection. SSE delivers commands or
notifications, with results submitted through POST requests. WebSockets can
carry commands, acknowledgments, results, and liveness messages in both
directions over one connection.

Advantages:

* Low delivery latency and less repeated HTTP request overhead.
* Supports frequent progress events naturally.

Disadvantages:

* Requires connection lifecycle handling, reconnect/replay behavior, and
  deployment support for streaming or protocol upgrades.
* Internal push delivery must reach the API replica owning the connection;
  gateway-initiated retrieval instead incurs internal polling or waiting costs.
* Standard WSGI does not provide a portable WebSocket interface; synchronous
  SSE still occupies a worker while waiting.
* Persistent connections do not remove command-delivery uncertainty or provide
  durable workflow state.

SSE-specific trade-offs:

* Results retain ordinary HTTP submission and its request-level handling.
* Intermediary response buffering must be disabled or bounded to preserve
  delivery latency. Reconnect event IDs alone do not provide durable replay.

WebSocket-specific trade-offs:

* Bidirectional application messages share one connection.
* Message framing, protocol version selection, reconnect behavior, and
  connection-scoped authentication must be specified explicitly.

Prerequisites / blockers:

* The deployed proxy/backend path must support the selected streaming protocol,
  idle-connection policy, and graceful connection draining with reconnects.
* Work acquisition and result ingestion must continue independently of a slow
  socket writer, with bounded queues and a defined overload response.

A localized ASGI backend can be placed behind the existing public API proxy::

  Public API address
       |
       +-- agent-channel routes -> ASGI backend
       |
       +-- existing routes -----> Pecan/WSGI backend

Alternatively, an ASGI server can host new routes and wrap the existing WSGI
application with an adapter. That preserves controllers but changes hosting
for every existing request and requires validation of adapter concurrency,
middleware, request contexts, and shutdown behavior.

Command delivery versus wake-up notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A stream can carry full commands or only notify IPA that pending work may be
available. In the latter design, IPA fetches commands and submits results
through the HTTP exchange contract. Notifications are recoverable hints, not
command delivery acknowledgments.

This keeps command semantics in REST and can reduce idle polling. It requires
a fallback check after reconnect and periodic reconciliation if notifications
can be lost. It also adds a fetch round trip. Routing or retrieving notifications
still requires an internal delivery mechanism.

gRPC bidirectional streaming
--------------------------------------------------------------------------

IPA can open a bidirectional streaming RPC to a gateway behind the public API
address. Commands travel toward IPA and acceptance responses, results, and
events travel back over that stream.

Unlike SSE, both WebSockets and a gRPC bidirectional stream can carry the
entire exchange on the same connection. Neither requires the conductor to
initiate a network connection to IPA.

Advantages:

* Protobuf and generated clients provide explicit message and service schemas.
* gRPC supplies streaming APIs and HTTP/2 transport flow control.
* A structured protocol can describe commands, acceptance, results, progress,
  and protocol evolution consistently.

Disadvantages:

* Introduces service definitions, code generation, dependencies, and a
  gRPC-capable hosting/proxy path.
* Existing flexible JSON command parameters need an explicit representation;
  schema generation does not automatically type every hardware extension.
* A server writing command messages to an IPA-opened stream is not an ordinary
  server-initiated gRPC method call. Commands multiplexed on the stream still
  require application-level correlation IDs.
* Stream flow control does not replace per-agent work limits, durable
  acceptance, duplicate handling, or recovery after a lost response.
* Internal work acquisition has the same push-routing or gateway-pull choices
  as WebSockets.
* Message-size limits must accommodate the selected payloads or use a separate
  transfer path. Long-lived streams also need explicit load-balancing and
  draining behavior; adding replicas does not redistribute existing streams.
* Generated schemas still require compatibility rules for unknown fields,
  enum evolution, and flexible extension payloads.

Prerequisites / blockers:

* The deployed HTTP/2 proxy/backend path and IPA image must support the chosen
  gRPC stack, streaming behavior, and compatible service versions.
* The persistent-channel capacity and lifecycle conditions described for
  SSE/WebSockets also apply.

For a minimal transport adaptation, WebSockets can reuse existing JSON command
envelopes. gRPC is worth revisiting if generated clients or a more strongly
typed cross-language protocol becomes a concrete requirement. For the initial
implementation, flexible hardware payloads limit that benefit while the new
stack adds work. Neither choice requires converting the existing REST
controllers or the whole conductor to asyncio.

Persistent channel with gateway-initiated work retrieval
--------------------------------------------------------------------------

The gateway holding an IPA connection can retrieve work from the responsible
conductor and forward it over the external stream::

  IPA -- opens stream --> gateway
                             |
                             +-- requests work --> conductor
                             <-- command in RPC response --
  IPA <-- command -----------+

The gateway can use short internal polling or bounded long-poll RPC. This
combines a persistent external channel with the internal retrieval mechanisms
already described. Results return through a separate internal call, whose
execution capacity must remain available while work retrieval waits.

Advantages:

* The outstanding RPC supplies the return path to the gateway. The conductor
  does not need an agent-to-gateway owner registry for command delivery.
* Either existing RPC backend can carry work retrieval and result submission.
* External streaming does not require reverse gateway RPC or a broker mailbox.

Disadvantages:

* Short internal polling retains its request load and delivery latency.
* Blocking internal long polling retains its waiting execution costs, even
  when the external connection uses asynchronous I/O.
* Reconnects and overlapping retrieval requests still require session fencing,
  bounded delivery, and duplicate handling.
* Retrieval must stop or reduce delivery credit when the outgoing queue is
  full. Otherwise a bounded socket queue only moves the backlog into claimed
  commands or pending internal calls.

Prerequisites / blockers:

* The selected short- or long-poll internal retrieval mechanism must meet its
  own capacity conditions, independently of external streaming capacity.
* Gateway failure after retrieval but before forwarding must leave commands
  recoverable under the selected acknowledgment contract.

A pull-based gateway need not have an internally addressable callback endpoint.
This removes reverse routing requirements, not the need to establish which
session or connection generation may receive new work.

A WebSocket frontend with blocking internal retrieval retains the internal
waiter bottleneck while adding stream lifecycle work. Select this combination
for a demonstrated external transport benefit, not as an automatic scaling
improvement over end-to-end HTTP long polling.

Push routing to the process that owns a persistent connection
--------------------------------------------------------------------------

Advantages:

* Avoids periodic work-retrieval calls for idle agents.
* Can deliver promptly without holding an internal retrieval RPC open.

Disadvantages:

* Adds connection-owner routing and its failure modes.
* Shared routing queues can couple congestion and failures across agents.

Prerequisites / blockers:

* Authoritative ownership and command-validity rules must prevent stale
  gateways from causing unauthorized execution after reconnect or takeover.
* Internal delivery must reach the owning process, with bounded queues and
  recoverable handling of failed handoffs.

For internal push delivery to a persistent channel, work must be routed to the
gateway process holding the agent connection. Gateway-initiated retrieval is
the alternative described above. Push routing does not need a remotely
addressable Python thread ID. Within the gateway process, a local registry maps
agent execution identity to connection state::

  Internal command receiver
       -> local connection registry
       -> bounded per-connection outgoing queue
       -> socket writer task

A dedicated writer task serializes writes and prevents a slow agent from
blocking delivery to other agents. If an internal receiver runs on a different
native thread, it hands work to the owning event loop using thread-safe
scheduling, such as ``loop.call_soon_threadsafe`` or
``asyncio.run_coroutine_threadsafe``. It must not directly manipulate an
``asyncio.Queue`` or socket from the other thread. Handoff must preserve queue
bounds rather than creating an unbounded backlog of scheduled callbacks.

Execution session and connection generation should be distinguished. An IPA
execution session can survive a network reconnect, while each new connection
has a new generation. When IPA reconnects through gateway B while gateway A
still considers its socket live, an authoritative ownership transition must
withdraw A's authority to acquire new work. An ownership check cannot recall
bytes already sent or eliminate a race between checking and writing. Agent-side
validity checks, command deduplication, and reconciliation must handle late
delivery. Late results must be reconciled against the command and execution
identity rather than blindly accepted or discarded.

RabbitMQ routing layouts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two broker layouts are possible:

Per-agent queue
  The gateway owning an agent connection consumes that agent/session's queue.
  The conductor publishes to the agent identity without knowing the gateway.
  This is simple and can buffer work across disconnection, but requires a
  queue/consumer lifecycle for each agent. An exclusive or single-active
  consumer does not by itself make the newest connection authoritative;
  reconnect ownership still needs fencing.

Per-gateway-process queue
  Each gateway process consumes its own queue. A direct exchange binds each
  connected agent/session routing key to the owning process's queue. The
  gateway dispatches messages locally to the corresponding socket. This
  reduces queue count but introduces per-agent bindings and shared queue
  resource limits. It provides live routing, not automatically durable
  per-agent buffering.
  Congestion or failure of that queue affects multiple agents; fairness and
  delivery limits must prevent one slow agent from exhausting shared capacity.

For the second layout, overlapping bindings during reconnection are important:
a direct exchange routes to every matching bound queue, not just the newest
owner. Binding updates alone are not a complete ownership protocol. The design
must also handle publication before a binding exists and stale ownership after
a gateway failure.

Responses need a return destination as well as a command ID. A conductor-side
receiver can resolve pending synchronous calls. Its execution capacity must
remain available even when workflow workers are blocked awaiting replies.
A reply destination belonging to a conductor process solves live request/reply;
it does not by itself recover commands after that process restarts.

JSON-RPC routing layout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Without RabbitMQ routing, internal push delivery to a persistent channel needs
an explicit connection owner registry or equivalent routing service::

  agent execution identity
       -> gateway process endpoint + connection generation

  conductor -> internal gateway request -> owning process -> IPA socket

Each owning gateway process must be addressable internally, or another service
must forward to it. Sending the command to an ordinary load-balanced API
address does not ensure that it reaches the owning process.

This adds ownership registration/expiry, internal gateway endpoints,
stale-owner fencing, and correlated response handling. Ironic's current
conductor JSON-RPC abstraction does not automatically supply reverse routing
to API gateway processes. Reusing JSON-RPC as an encoding or client mechanism
would not remove the need to design those responsibilities.

A RabbitMQ-only mode would add a deployment dependency for JSON-RPC-only
installations. The recommended initial exchange preserves both RPC backends.

Return-path comparison
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* End-to-end long polling: the outstanding RPC carries the response to the
  API process already holding the agent request.
* Persistent channel with gateway pull: the outstanding retrieval RPC carries
  work to the gateway holding the stream, with either existing RPC backend.
* Persistent channel with RabbitMQ push: consumers or exchange bindings route
  work to the owning gateway process.
* Persistent channel with JSON-RPC push: explicit owner discovery and an
  internal gateway request route are required, or an equivalent routing service.

Persistent channels can preserve existing request/reply callers. Both
end-to-end long polling and gateway-initiated retrieval provide a return path
across the existing RPC transports. The choice should account for execution
semantics, internal retrieval costs, and process-level routing.

Database-backed command mailbox
--------------------------------------------------------------------------

The conductor records pending commands and their workflow association in the
database. An API exchange retrieves eligible work and records results::

  Workflow -> transaction: command + workflow association
                                  |
  API exchange -> claim/read pending command -> IPA
  IPA result  -> record correlated result -> schedule continuation

Retrieval can use short conductor RPC calls. Direct API access through a
purpose-built database layer is a separate placement choice that requires
explicit authorization, ownership, and transaction boundaries. Neither choice
is an existing agent mailbox implementation.

Advantages:

* Command creation and its workflow association can share a transaction.
* Pending work survives gateway disconnection without a mandatory broker.
* Pull delivery does not require discovery of the gateway owning a connection.
* Inspectable command history and reconciliation can use the existing
  operational datastore.

Disadvantages:

* Requires atomic eligibility and acceptance rules, result correlation,
  retention, and coordination with conductor ownership and takeover.
* Polling and result ingestion add database load and possible contention.
* Payload storage, indexes, retention, and cleanup add database capacity and
  maintenance costs.
* A delivery lease expiring does not establish whether IPA executed a command;
  stable command identities and unknown-outcome recovery remain necessary.

A full delivery-lease system is not mandatory for a single-command protocol.
Repeatedly returning the same eligible command until acceptance can be simpler
if IPA deduplicates atomically and the server enforces ordering. Competing
consumers or pipelining can justify stronger claims or leases. A pull design
does not inherently need a separate gateway-owner registry.

Sharing a database does not automatically make command creation and workflow
advancement atomic. Existing node saves and ``TaskManager.process_event()``
must be integrated with the selected transaction or reconciliation boundary.
The result can wake a correlated caller or schedule a continuation; database
storage does not dictate the workflow execution model.

Prerequisites / blockers:

* Transaction and ownership rules must prevent conflicting command creation,
  delivery claims, and workflow advancement during takeover.
* The supported database backends must sustain the selected query and write
  rates, with bounded payload storage and retention.

Wake-up notifications can accelerate delivery without becoming authoritative:
commit command state, notify interested waiters, and recover missed
notifications by rechecking that state. Long polls and streams can use this
pattern. Subscription and state checks must avoid missed wake-ups, and bounded
rechecks must recover lost notifications. Notification routing is still a
design responsibility, but durable command delivery need not depend on it.

RabbitMQ-backed per-agent mailbox
--------------------------------------------------------------------------

The conductor publishes to an exchange with a routing key identifying a node
and agent execution session. An async API bridge consumes the corresponding
queue and delivers commands through polling or a stream::

  Conductor -> RabbitMQ -> async API bridge -> IPA-initiated connection

Authentication should use a request header available to IPA's Python client.
If a token is placed in the channel path, URL logging must be redacted.
The token should resolve to an identity and mailbox; the raw credential need
not become the queue name.

Advantages:

* RabbitMQ routes work to the consuming API replica.
* Broker buffering decouples publication from an active agent connection.
* Existing RabbitMQ installations can supply the transport infrastructure.

Disadvantages:

* Introduces per-agent queue creation, retention, consumer ownership, and
  cleanup responsibilities.
* Requires RabbitMQ for this mode; Ironic deployments using JSON-RPC alone do
  not automatically have that dependency.
* oslo.messaging's service RPC interface does not directly expose the required
  application-controlled delivery acknowledgment contract.
* Broker acknowledgment, IPA acceptance, and command completion are distinct.
* Database state and broker publication require an outbox or another explicit
  reconciliation strategy if durable consistency is required.
* Retention and availability during broker failure depend on queue type,
  replication policy, and acknowledgment behavior. Queue count also affects
  broker recovery and failover costs.

Prerequisites / blockers:

* RabbitMQ must be an accepted dependency for this mode. A deployment required
  to operate with JSON-RPC alone cannot select this mailbox.
* Queue type, replication, publication confirmation, and reconciliation must
  meet the same recovery guarantees used to assess a database mailbox.

One implementation could use a synchronous AMQP client such as Kombu on the
conductor side and an asyncio client such as aio-pika in the bridge, with an
explicit shared envelope. This would be new integration, not reuse of
oslo.messaging's private RabbitMQ driver internals.

Queue lifetime should span temporary API disconnections. A connection-exclusive
queue is therefore usually unsuitable. A nonexclusive queue with a controlled
single consumer can retain pending work while limiting concurrent delivery.
Durability, publisher confirms, unroutable-message detection, and cleanup
must be specified rather than inferred from using RabbitMQ.

Reverse tunnel carrying existing IPA HTTP
--------------------------------------------------------------------------

IPA opens an outbound tunnel to the API gateway. An internal relay forwards
the conductor's existing IPA HTTP requests through that tunnel. The IPA API
listener can be restricted to a local interface.

Advantages:

* Preserves synchronous calls, command formats, extensions, and status polling.
* Minimizes changes to conductor workflow logic.
* Addresses connectivity without requiring a command-mailbox architecture.

Disadvantages:

* Requires tunnel ownership, internal routing, authentication, and request
  multiplexing or explicit serialization.
* Retains existing request volume and lost-response ambiguity.
* Adds a relay service path and its failure modes.

Prerequisites / blockers:

* A local IPA HTTP listener must satisfy the deployment requirement. This
  option addresses network reachability but does not eliminate the listener.
* The gateway must route authenticated requests to the correct tunnel, and
  reconnects must not silently replay requests with unknown outcomes.

A transparent tunnel preserves existing HTTP semantics; it does not itself add
stable command identities or duplicate-safe execution. Comparing it with a
durable mailbox requires an explicit choice: accept existing lost-response
limitations, or extend the agent protocol with correlation and deduplication.
The latter reduces the tunnel's minimal-change advantage. The common recovery
contract below is a target that a tunnel must implement or explicitly qualify.

A reusable relay can make this the lowest-change connectivity solution.
Implementing a new highly available relay still requires routing, lifecycle,
authentication, and failure propagation; small workflow changes do not imply
small total effort.

Complete design comparison
==========================

The following combinations summarize complete designs. Effort depends on the
supported workflows and recovery guarantee, not just the external protocol.
The B+C polling combination is the recommended first prototype; the remaining
combinations have specific conditions that would justify further evaluation.

.. list-table:: Candidate combinations and feasibility gates
   :header-rows: 1
   :widths: 25 35 40

   * - Combination
     - Main benefit and cost
     - Feasibility gate
   * - Inline processing with short HTTP polling
     - Few transport components; call sequences need adaptation.
     - Every supported transition can return before requiring its command's
       result. Lost-response recovery still needs defined state.
   * - Scheduled workflows with blocking long polling and synchronous callers
     - Preserves scheduling and call sites; retains blocked execution capacity.
     - Delivery and replies progress independently of caller locks and pools.
       Restart recovery cannot depend on the old caller's stack.
   * - Database mailbox, continuations, and short HTTP polling
     - Durable workflow association without a broker; adds transitions and
       database traffic.
     - Crash-safe transitions, continuation compatibility, and polling capacity
       meet the target. Notification-assisted delivery can be added separately.
   * - Persistent channel with gateway pull and correlated synchronous calls
     - Reuses RPC return routing; combines stream lifecycle costs with internal
       polling or waiting costs.
     - Both internal retrieval and caller-wait capacity are sufficient. A
       selected durable store or reconciliation policy handles restarts.
   * - Persistent channel with RabbitMQ mailbox and continuations
     - Buffered push delivery; adds queue operations and database/broker
       reconciliation.
     - RabbitMQ is accepted, ownership is fenced, and publication plus workflow
       advancement meet the selected recovery guarantee.
   * - Reverse tunnel with existing IPA HTTP
     - Small workflow changes; retains relay and request/reply failure modes.
     - A local listener is acceptable. Existing recovery limitations are
       accepted explicitly or the protocol gains equivalent recovery semantics.

Compare these combinations using the same active-agent population, delivery
latency target, supported workflows, and restart/unknown-outcome guarantees.
A process-local prototype and a durable production design have different
guarantees even when they use the same external transport.

Effort assessment
-----------------

Scheduled workflows with correlated HTTP exchange
  Concentrates changes in command transport, IPA communication, command state,
  and independent reply dispatch. Existing asynchronous step wait states remain
  useful. This is the preferred incremental design if lock-independent dispatch
  is feasible and waiting capacity meets the deployment target.

Inline processing
  Has few transport components but potentially extensive workflow changes.
  Result-dependent sequences need explicit resumption or another context. Use
  only for narrowly audited paths already shaped as one transition per exchange.

Continuations with durable command state
  Adds workflow phases, recovery boundaries, and continuation compatibility.
  It offers the strongest worker-utilization model but can dominate project
  effort. Apply selectively when lock occupancy or automatic recovery warrants
  it rather than converting all workflows initially.

Streaming with gateway pull
  Adds hosting and connection lifecycle without removing internal polling or
  waiting costs. Revisit if frequent events or external request overhead
  justify a persistent channel.

Streaming with broker push
  Adds queue or binding lifecycle, ownership fencing, and database/broker
  consistency work. Revisit if idle internal retrieval is a measured bottleneck
  and RabbitMQ is an accepted dependency. An existing RabbitMQ installation
  alone does not establish a benefit over database-backed command state.

Reverse tunnel
  Preserves the most existing behavior. Total effort is attractive when a
  suitable relay can be reused, but uncertain for a new highly available relay.
  It meets the connectivity requirement only if retaining a local listener and
  the selected lost-response behavior is acceptable.

Delivery and recovery semantics
===============================

Command acknowledgment
----------------------

Returning an HTTP response or completing a WebSocket send does not prove that
IPA accepted a command. Commands should have stable identities, and repeat
delivery should retrieve the same execution status instead of starting a
second execution.

IPA must make duplicate detection and admission to execution atomic. A stable
ID alone does not prevent concurrent deliveries from both starting. The
acceptance record and command-history lifetime must cover the promised retry
window. Loss of that history after reboot requires session reconciliation.

For a broker-backed channel, a useful sequence is::

  Publish C -> deliver C -> IPA accepts C -> acknowledge broker delivery
                                     ... execute ...
                             IPA reports result C

Acknowledging on acceptance avoids holding a broker delivery unacknowledged
for a long erase or deployment. It requires separate tracking and recovery
after acceptance. Acknowledging only on completion is another choice, but
couples execution to consumer lifetime and broker acknowledgment timeouts.

Acknowledgment on acceptance transfers recovery responsibility away from the
broker. Ironic must retain the accepted command's workflow association, and IPA
must retain enough execution status to report it after reconnect. If IPA loses
that status, recovery must handle an unknown outcome rather than assume the
broker can redeliver the accepted command safely.

Results should be acknowledged to IPA only after durable acceptance when the
chosen design promises recovery across server restarts. Duplicate result
reports must be harmless.

Ordering and delivery credit
--------------------------------------------------------------------------

The contract must define how many commands may be outstanding for one agent,
whether status and control requests may overlap command execution, and whether
dependent commands require completion of their predecessors. Broker prefetch
and socket queue bounds do not establish these application-level rules.

Concurrent or replacement polls must have defined behavior: reject an extra
waiter, replace the previous waiter, or permit bounded concurrent delivery with
explicit correlation and ordering. A lost response must not allow a retry to
skip an unaccepted command or unintentionally execute dependent work early.

Workflow invalidation and cancellation
--------------------------------------------------------------------------

An agent execution session can outlive an aborted or replaced workflow. Session
identity alone therefore does not make every queued command current. Commands
need a workflow generation or equivalent validity boundary, with rules for
invalidating queued work and rejecting stale results within the same agent boot.

The design must define the race between invalidation and agent acceptance,
including commands already delivered but not yet started. Preventing delivery
of queued work, cancelling accepted work, and interrupting running work are
different operations. If cancellation of running commands is unsupported, the
workflow must reconcile their outcomes before issuing conflicting work.

Restart and takeover
--------------------

A process-local rendezvous is sufficient to demonstrate the happy path, but
not to promise seamless restart or takeover. The design must decide which
state is persisted, how a new conductor recovers it, and what IPA reports
after reconnecting.

Agent reboot can erase IPA's in-memory command history. A new boot must not
blindly execute an old destructive command. Possible mechanisms include an
explicit session ID, an existing token lifecycle with verified equivalent
semantics, or a reconciliation exchange.

Exactly-once execution cannot be assumed merely because command IDs or a
durable broker are present. Recovery policy must address unknown outcomes.

Ownership checks must also fence command producers, not just delivery. A stale
conductor can finish slow work after another conductor takes over. It must not
commit new commands or advance the workflow after losing authority. Command
creation and advancement must validate the expected workflow generation and
applicable ownership at their commit boundary. An outbox solves publication
consistency, not this authority check.

Liveness and progress
---------------------

The following facts should remain distinguishable:

* The agent recently contacted the API.
* A command was delivered and accepted.
* The command is running or making progress.
* Its result was accepted by Ironic.
* The conductor resumed the workflow.

A healthy heartbeat or stream does not prove command progress. If one
long-poll request remains open while a command finishes, IPA must be able to
report that completion promptly, for example through a separate result POST
or a defined replacement/cancellation behavior. Otherwise long polling could
delay the existing forced-completion notification.

Authenticated contact should be recordable independently of workflow lock
acquisition. Otherwise conductor contention can make a communicating agent
appear stale. Frequent polling must not require a contended node-row write on
every exchange; coalesced timestamps or separate session state are options.

Large payloads and control-message priority
--------------------------------------------------------------------------

The contract must bound command, capability, progress, and result payloads.
Log collection in particular needs a choice between bounded channel messages
and a separate upload path reachable from IPA. A separate upload requires its
own authorization, retry, and result-reference lifetime rules.

Large transfers must not indefinitely delay liveness, acknowledgments, or
control messages. Stream flow control and a bounded outgoing queue alone do
not provide this priority. Chunking, scheduling, or separate transfer paths
must preserve it, including when a peer is slow. IPA must also bound retained
unacknowledged results during an outage without silently losing required data.

Connectivity changes during workflows
--------------------------------------------------------------------------

Provisioning-network changes, reboot, and handoff can interrupt a channel that
worked during bootstrap. The workflow must define when a disconnect is expected,
how IPA discovers and reconnects to the API afterward, and how pending commands
and results are reconciled. Every phase requiring further exchanges needs a
reachable API path from that phase's network configuration.

Loss of connectivity alone is not proof that a network-changing or terminal
operation succeeded. Completion rules must identify any independent evidence
required before advancing, and distinguish reconnect within an execution
session from a new agent boot.

Upgrades and backwards compatibility
====================================

Activation requirements
-----------------------

Activation requires supported Ironic and IPA versions, explicit opt-in, and
REST microversion detection. Existing direct communication remains the default.
Use the minimum bootstrap mechanism that establishes a retry-consistent
transport and execution identity before any commands are issued.

Configuration and negotiation alternatives
--------------------------------------------------------------------------

Operator-selected transport
  Select transport per deployment, conductor group, or node, and pass it to
  IPA in boot configuration. This is simple in controlled installations but
  requires coordinating image support and conductor eligibility.

Lookup-selected transport
  IPA advertises support through a new microversion or request field. Lookup
  returns the selected transport and wait policy. This reuses the existing
  bootstrap exchange but requires persistent, retry-consistent selection.

Agent registration endpoint
  After lookup, a new IPA explicitly registers for polling before normal
  heartbeat-driven processing can issue direct commands. This isolates the
  new behavior but adds an idempotent registration operation and bootstrap
  ordering constraints.

Explicit session negotiation
  Advertise supported transports and protocol versions, select one, and bind
  it to a persisted agent session. This provides a clear execution boundary
  but adds state and lifecycle machinery that may exceed initial needs.

Separate deploy interface
  Introduce an opt-in interface for experiments. This provides a visible
  rollout boundary but transport also affects cleaning, servicing, rescue,
  and inspection; a deploy interface alone is not a complete boundary.

Compatibility matrix
--------------------

* Old Ironic and old IPA continue using direct HTTP.
* New Ironic must continue accepting old IPA in the legacy mode.
* New IPA can use old Ironic through the legacy mode only when policy and
  network connectivity permit fallback.
* The new mode activates only when both sides and eligible conductors support
  the required protocol.

Fallback should happen at bootstrap, before command execution. A failure after
delivery must trigger reconciliation, not silent transport switching.
Policies such as ``prefer-agent-poll`` and ``require-agent-poll`` distinguish
optional adoption from installations where direct HTTP cannot work at all.

Internal rolling upgrades
-------------------------

An upgraded API advertising a microversion is not enough: a node may be owned
by, or fail over to, an older conductor. The simpler initial policy is to enable
the feature only after all conductors eligible for the affected nodes support
it. Mixed-capability ownership would require additional routing and takeover
rules.

New RPC methods or arguments need versioning and release mapping updates.
Any schema additions should be additive and deployed before activation.

Disabling the feature should stop creation of new polling sessions. Existing
sessions must finish or be explicitly terminated before downgrading to code
that cannot handle them. Updating software and activating the transport should
be separate rollout steps.

Data model impact
=================

A process-local design may avoid schema changes but cannot promise durable
command or result recovery. Persistence requirements follow from the selected
recovery guarantee rather than from the choice of short or long polling.

A durable rendezvous design may require records for:

* Selected transport and agent execution identity.
* Command identity, workflow association, payload, and delivery state.
* Accepted results and reconciliation state.

A RabbitMQ queue does not replace the need to associate work with the node's
persisted workflow. Exact schema and retention policy depend on the selected
recovery guarantees and should be designed after the command call-path audit.

Database-backed command state and optional wake-up notifications are an
alternative to broker-backed buffering. Session identity, workflow generation,
and delivery ownership have distinct lifecycles even when stored together.

State machine impact
====================

Existing deployment, cleaning, and servicing wait states provide useful
continuation boundaries. New public provisioning states are not necessarily
required, but synchronous command sequences may need internal continuation
state.

The implementation must distinguish an empty work poll from an instruction to
advance a step. Repeated polls and duplicate results must not cause duplicate
workflow execution. Rescue and terminal operations such as reboot, power-off,
and lockdown need explicit completion rules when the agent intentionally
disappears before responding.

REST API impact
===============

Two primary choices are extending heartbeat under a new microversion or adding
a dedicated work-exchange endpoint. Both need defined request/response schemas,
no-work behavior, identity validation, capability reporting, and maximum wait
semantics.

Short and long polling should share command and result semantics. A new ASGI
route is necessary only if the selected implementation needs asynchronous
connection handling; a synchronous Pecan endpoint can implement bounded
blocking long polling.

A route served outside the WSGI application does not inherit Pecan hooks and
middleware automatically. Authentication, policy, ramdisk-endpoint enablement,
request context, request IDs, and protocol-version handling must be integrated.

RPC API impact
==============

A blocking exchange can use a versioned conductor method conceptually shaped
as ``exchange_agent_work(identity, reports, wait_timeout)``. Its contract must
specify when reports are accepted, whether workflow work is scheduled, what
ends the wait, and how timeouts map to REST responses.

``wait_timeout`` controls the receiver's software work wait. Expiry produces
a normal no-work result. It does not request a JSON-RPC or oslo.messaging
transport timeout. The configured outer timeouts must allow sufficient margin;
a new per-call transport timeout interface is not required.

Preserving ``ConductorAPI`` avoids requiring RabbitMQ in deployments using
JSON-RPC. A dedicated broker mailbox is a separate transport choice rather
than a necessary implementation of this RPC.

Driver API impact
=================

``AgentClient`` is the principal abstraction boundary to investigate.
Transport selection can be introduced at ``get_client(task)``, but a new
backend must preserve or explicitly revise synchronous result expectations.

The call-path audit should cover at least:

* Clean, deploy, and service capability discovery.
* Asynchronous step execution and last-command result retrieval.
* Bootloader and partition information operations.
* Rescue finalization.
* Log collection, synchronization, shutdown, reboot, and lockdown.

The audit should identify commands that can be returned directly, data that can
be reported and cached, and calls requiring a result-driven continuation or a
blocked caller. No transport should report synthetic success when only command
submission has succeeded.

Ramdisk impact
==============

IPA needs bootstrap selection, work retrieval, command correlation, result
reporting, timeout/retry handling, and optional omission of its network API
listener. Existing command execution extensions should remain reusable.

Communication and command execution need independent contexts. Synchronous
extensions must not block the poller, socket reader, or result reporter. In the
inspected IPA, command IDs are generated by the agent and execution history is
in memory; a native exchange needs sender-stable identity and atomic
deduplication before execution.

Listener lifecycle is also command behavior. In the inspected IPA, rescue
finalization disables ``serve_api``; lockdown sets the lockdown flag and
disables ``serve_api``. Ironic's current lockdown path checks for subsequent
agent connection failure. A native channel must define channel shutdown,
command rejection, and final result handling explicitly. Disabling an unused
HTTP listener must not leave a supposedly locked-down agent controllable.

Capability reports should be bounded and versioned or sent on demand rather
than automatically repeating full lists on every exchange. If data depends
on node configuration as well as agent version, that dependency must be part
of the refresh contract.

Security impact
===============

The change removes conductor-to-agent connection requirements. Existing agent
authentication remains necessary in the new direction. Credentials must be
bound to the permitted node/execution context, and incoming results must not
be allowed to advance unrelated work.

Tokens embedded in URL paths require redaction throughout proxy and service
logging. Broker routing identities should not expose raw bearer tokens.
Public exposure remains limited to the API; broker and conductor services
remain internal.

Existing command payloads can contain secure node data, configdrives, or rescue
credentials. Persisting them changes their lifetime and exposure. Minimize
stored payloads, restrict access, redact logs, and define retention and cleanup.
Long-lived channels also need explicit credential-expiry and revocation
behavior and authenticated TLS server verification.

Performance and scalability impact
==================================

Short polling increases request frequency. Long polling reduces external
request churn but consumes waiting capacity in synchronous servers. Streaming
reduces churn further but introduces connection lifecycle and reconnection
load. Internal push adds owner-routing costs; gateway pull retains internal
polling or waiting costs. A broker mailbox adds queue and consumer resource
costs. A database mailbox adds query, transaction, and retention costs.

Measurements must include simultaneous idle agents, active command execution,
lock contention, conductor overload, mass reconnect, and multiple API replicas.
Normal and reserved conductor workers are not an unlimited waiting pool.

Timeouts should be treated as wait budgets, not extended indiscriminately to
hide overload. Empty responses may include retry guidance. A long-poll timeout
alone must not cause an agent to be classified as dead or its command to fail.

Other impacts
=============

No user-facing CLI, SDK, or Nova driver change is inherently required for a
server-configured transport. Per-node controls would require corresponding
API exposure and may warrant client support.

Operators need deployment documentation for timeout ordering, supported
transport combinations, mixed-version activation, and fallback policy.
Integrated inspection needs its own call-path and listener-lifecycle audit
before support can be claimed.

Implementation
==============

Assignee(s)
-----------

None assigned. This document records alternatives for discussion.

Work items
----------

#. Audit command call paths and identify their lock and result expectations.
#. Audit a pinned, current IPA revision for command execution, command-history
   lifetime, deduplication, listener dependencies, and concurrent work/result
   handling. Record the revision and turn assumptions into feasibility tests;
   the previously inspected sibling checkout does not complete this check.
#. Confirm the initial workflow scope, active-agent population, delivery-latency
   target, and restart/unknown-outcome guarantee.
#. Define the exchange, acknowledgment, empty response, and recovery contract
   for the recommended B+C HTTP implementation.
#. Choose the minimum bootstrap and upgrade mechanism that satisfies the
   deployment requirements.
#. Prototype independent delivery and reply dispatch with a capability request,
   asynchronous step acceptance and completion, a completion-polled operation,
   and rescue or lockdown. Demonstrate progress while the caller holds its
   node lock.
#. Exercise zero-wait and receiver-bounded long polling through the same
   implementation on both remote RPC backends.
#. Add restart/takeover support matching the documented guarantees.
#. Measure resource use before selecting additional asynchronous hosting or
   broker infrastructure.
#. Convert only the synchronous sequences whose measured lock occupancy or
   recovery requirements justify continuations. Reconsider other transports
   only if the polling design fails a stated capacity or latency target.
#. Document operation and add an end-to-end deployment/cleaning test job.

Validation and testing
======================

Tests should establish behavioral invariants rather than merely mirror the
chosen implementation:

* Publication before, during, and immediately after a poll's wait returns work
  correctly without a missed wake-up.
* When no command is available, the poll returns a bounded no-work response.
* Spurious or repeated wake-ups do not extend the monotonic work-wait deadline.
  Both RPC backends return normal no-work results on receiver-side expiry
  without depending on per-call transport timeout overrides.
* A dropped response can be retried without duplicate execution.
* Duplicate results do not advance a workflow twice.
* A report committed before a work wait survives timeout or response loss.
  Its retry is acknowledged even after workflow advancement, without repeating
  the transition.
* Inline processing returns command delivery before requiring that command's
  result; preserved synchronous callers use an independent delivery path.
* Concurrent polls and expired delivery leases preserve command identities,
  delivery limits, and dependency ordering.
* Workflow abort or replacement within one agent boot invalidates obsolete
  work and reconciles the race with command acceptance.
* Lost wake-up notifications are recovered by bounded state rechecks.
* Gateway-pull delivery returns work to the requesting gateway across supported
  RPC backends without a reverse gateway endpoint.
* If agent-executed plans are selected, partial failure and interruption follow
  the declared stop, resume, and per-operation reporting rules.
* Waiting requests do not hold locks or exhaust capacity required for command
  publication and result ingestion.
* A reply wakes a workflow caller that holds an exclusive node lock without
  requiring that lock in the reply handler. Duplicate reports do not launch
  competing workflow processing while that caller is active.
* Stale conductors cannot commit commands or workflow advancement after losing
  authority. Late gateway delivery cannot bypass command-validity checks.
* A full outgoing queue stops further retrieval or reduces delivery credit
  without creating an unbounded backlog elsewhere.
* Large payloads and slow transfers preserve bounded control-message latency.
* Network-changing operations reconcile expected disconnection and reconnect
  without treating loss of connectivity as proof of success.
* RPC timeout and HTTP disconnect leave bounded server work and recoverable
  command state.
* Command completion is reported promptly even if a work poll is outstanding.
* Reboot and stale-session reports cannot apply results to a newer workflow.
* Supported API and conductor replicas can handle reconnects and takeover.
* Old/new IPA and Ironic combinations follow the documented compatibility
  matrix, including internal RPC version caps.
* Supported upgraded conductors can resume persisted continuations created by
  older supported code.
* Feature disablement preserves active supported sessions until drained.
* Both oslo.messaging and JSON-RPC exercise the polling contract where claimed.
* Deployment, cleaning, servicing, rescue, and terminal commands are covered
  according to the declared feature scope.
* Synchronous IPA extensions do not block communication, and rescue or lockdown
  stops channel command acceptance according to its defined lifecycle.

Open decisions
==============

The first implementation needs decisions on:

#. Which workflows and restart guarantees are included initially, and where
   explicit unknown-outcome failure is acceptable.
#. How report admission validates identity and workflow association without
   acquiring a waiting caller's node lock.
#. Which database records and atomic boundaries provide command identity,
   accepted-result retention, and takeover reconciliation.
#. Whether existing token lifecycle semantics identify an agent execution
   safely, or a separate session identity is required.
#. How bootstrap selects the mode before direct commands can be issued.
#. How concurrent polls, status queries, workflow invalidation, and command
   acceptance interact under the single-side-effecting-command limit.
#. Which API and RPC waiter limits, report-handler capacity, and configured
   outer timeouts meet the active-agent and latency targets.
#. How large results and logs are transferred without delaying control traffic.
#. Which workflow phases change connectivity, and what establishes completion,
   channel shutdown, or a valid reconnect after each change.

Streaming protocol, broker layout, and general execution-plan choices are
deferred unless the first implementation demonstrates a requirement they
address. The execution-boundary audit and measured capacity determine whether
to revisit them.

Source references
=================

The implementation baseline and integration points above are the basis for the
technical assessment. Relevant IPA paths at the stated revision are
``ironic_python_agent/agent.py``, ``ironic_python_agent/api/app.py``, and
``ironic_python_agent/extensions/{base,rescue,system}.py``. Receiver timeout
integration concerns ``ironic/common/json_rpc/client.py``,
``ironic/common/json_rpc/server.py``, and ``ironic/common/wsgi_service.py``.

Related proposals provide additional background; the design does not depend
on their review outcome:

* `No Conductor to IPA Communication
  <https://review.opendev.org/c/openstack/ironic-specs/+/777172>`_
* `Agent heartbeat command and result RFE
  <https://storyboard.openstack.org/#!/story/2007765>`_
