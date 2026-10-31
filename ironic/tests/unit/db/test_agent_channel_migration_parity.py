#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
#    the License for the specific language governing permissions and
#    limitations under the License.

"""Unit test pinning the DDL of the agent channel journal migrations.

The DB unit tests build the SQLite schema from ``models`` and never
run the alembic migrations, so the migrations and the models can
drift on a column type that only bites on the backends the unit
tests do not exercise (for example TEXT vs LONGTEXT on MySQL).

This test executes the upgrade() of the journal migration (revision
dfb37433ec66) and of every successor revision against a stubbed
alembic ``op`` that records the ``sa.Table`` objects built by
``op.create_table`` and the columns added or dropped afterwards, and
compares the DDL type of every column of the three journal tables
between the migrations and ``models.Base.metadata``, compiled against
the MySQL and PostgreSQL dialects.

An applied revision is frozen: alembic never re-runs it, so a column
that appears only by editing an already applied revision is missing
from every existing database. The report slots of a command must
therefore come from a successor revision, and this test pins that
the applied revision declares only the original columns while a
single successor adds and drops the new ones.
"""

import importlib.util
import os

from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
import sqlalchemy as sa

from ironic.db.sqlalchemy import models
from ironic.tests import base

# The revision id of the migration that creates the journal tables.
MIGRATION_REVISION = 'dfb37433ec66'

AGENT_CHANNEL_TABLES = (
    'agent_channel_heads',
    'agent_channel_sessions',
    'agent_channel_commands',
)

# The report slots of a journal command.
REPORT_SLOTS = ('acceptance', 'receipt', 'result')

# The columns of agent_channel_commands as the applied journal
# revision created them. An applied revision is frozen, so this list
# must never change.
ORIGINAL_COMMAND_COLUMNS = (
    'created_at', 'id', 'invocation_key', 'name', 'params',
    'params_sha256', 'reply_boundary', 'sequence', 'session_uuid',
    'state', 'updated_at', 'uuid', 'version', 'workflow_epoch',
)

# The dialects the unit tests never exercise through a live schema.
DIALECTS = (mysql.dialect(), postgresql.dialect())


class _OpRecorder(object):
    """A stand-in for alembic ``op`` that records schema operations.

    ``create_table`` builds the same ``sa.Table`` that alembic's
    ``op.create_table`` builds from the same arguments, so the
    recorded table keeps the column types the migration declares.
    ``add_column`` and ``drop_column`` are applied to the recorded
    table and tracked, so a migration that evolves an existing table
    is captured the way alembic would apply it.
    """

    def __init__(self):
        self.metadata = sa.MetaData()
        self.tables = {}
        self.added_columns = {}
        self.dropped_columns = {}

    def create_table(self, table_name, *columns, **kwargs):
        self.tables[table_name] = sa.Table(
            table_name, self.metadata, *columns, **kwargs)

    def create_index(self, index_name, table_name, columns, **kwargs):
        # Index placement is not a type-parity concern.
        pass

    def drop_index(self, index_name, table_name, **kwargs):
        # Index placement is not a type-parity concern.
        pass

    def add_column(self, table_name, column):
        table = self.tables[table_name]
        if column.name in table.c:
            raise AssertionError(
                'the migration adds %s.%s, a column the recorded '
                'schema already declares' % (table_name, column.name))
        table.append_column(column)
        self.added_columns.setdefault(table_name, []).append(
            column.name)

    def drop_column(self, table_name, column_name):
        if column_name not in self.tables[table_name].c:
            raise AssertionError(
                'the migration drops %s.%s, a column that does not '
                'exist in the recorded schema'
                % (table_name, column_name))
        self.dropped_columns.setdefault(table_name, []).append(
            column_name)

    def __getattr__(self, name):
        raise AttributeError(
            'the migration-parity check records create_table, '
            'add_column and drop_column; the migration used op.%s'
            % name)


def _versions_dir():
    return os.path.join(os.path.dirname(models.__file__),
                        'alembic', 'versions')


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _revision_graph():
    """Every alembic migration, keyed by its revision id.

    Loads every version file and maps each revision id to its
    ``down_revision`` and its loaded module, so the test can find
    what follows the journal revision without a live database.
    """
    versions_dir = _versions_dir()
    graph = {}
    for name in sorted(os.listdir(versions_dir)):
        if not name.endswith('.py'):
            continue
        module = _load_module(name[:-3], os.path.join(versions_dir, name))
        revision = getattr(module, 'revision', None)
        if revision is not None:
            graph[revision] = {
                'down_revision': getattr(module, 'down_revision', None),
                'module': module,
            }
    return graph


def _down_revision_ids(down_revision):
    if down_revision is None:
        return ()
    if isinstance(down_revision, (list, tuple)):
        return down_revision
    return (down_revision,)


def _successors(graph, revision):
    """The revisions that directly follow ``revision``."""
    return sorted(
        rev for rev, entry in graph.items()
        if revision in _down_revision_ids(entry['down_revision']))


def _journal_chain(graph):
    """The journal revisions in upgrade order, the head last.

    Follows the single successor of the journal revision until the
    chain ends.
    """
    chain = [MIGRATION_REVISION]
    current = MIGRATION_REVISION
    while True:
        successors = _successors(graph, current)
        if len(successors) != 1:
            return chain
        current = successors[0]
        chain.append(current)
    return chain


def _run_migrations(graph, revisions):
    """Run the upgrade() of the given revisions on one recorder."""
    recorder = _OpRecorder()
    for revision in revisions:
        module = graph[revision]['module']
        module.op = recorder
        module.upgrade()
    return recorder


def _migration_tables():
    """The journal tables after the journal chain is applied.

    Runs the upgrade() of the journal revision and of every successor
    revision against a stubbed alembic ``op`` that records the DDL.
    A database that holds the journal revision gains later columns
    only through a successor, so the effective DDL is the
    accumulated result of the chain.
    """
    graph = _revision_graph()
    return _run_migrations(graph, _journal_chain(graph)).tables


class AgentChannelMigrationParityTestCase(base.TestCase):

    def _compiled_type(self, column, dialect):
        return str(column.type.compile(dialect))

    def _assert_column_types_match(self, table_name, migration_table):
        model_table = models.Base.metadata.tables[table_name]
        migration_columns = sorted(col.name for col in migration_table.c)
        model_columns = sorted(col.name for col in model_table.c)
        self.assertEqual(
            migration_columns, model_columns,
            'the migration and the models declare different columns '
            'for %s: migration=%s model=%s'
            % (table_name, migration_columns, model_columns))
        for column in migration_table.c:
            model_column = model_table.c[column.name]
            for dialect in DIALECTS:
                migration_type = self._compiled_type(column, dialect)
                model_type = self._compiled_type(model_column, dialect)
                self.assertEqual(
                    model_type, migration_type,
                    'the migration and the models disagree on '
                    '%s.%s on %s: model=%s migration=%s'
                    % (table_name, column.name, dialect.name, model_type,
                       migration_type))

    def _assert_single_successor(self, graph):
        successors = _successors(graph, MIGRATION_REVISION)
        self.assertEqual(
            len(successors), 1,
            'the journal revision %s is applied to existing databases '
            'and must not be edited in place; alembic needs exactly '
            'one successor revision with down_revision %s to carry '
            'columns that appear after the fact, found %s'
            % (MIGRATION_REVISION, MIGRATION_REVISION, successors))
        return successors[0]

    def test_migration_column_types_match_the_models(self):
        migration_tables = _migration_tables()
        for table_name in AGENT_CHANNEL_TABLES:
            self.assertIn(
                table_name, migration_tables,
                'the journal migration creates no table %s' % table_name)
            self._assert_column_types_match(
                table_name, migration_tables[table_name])

    def test_a_successor_revision_follows_the_journal_revision(self):
        # Alembic never re-runs an applied revision. A column that was
        # added to an applied revision after the fact is missing from
        # every existing database, so a successor must carry it.
        graph = _revision_graph()
        self._assert_single_successor(graph)

    def test_the_report_slots_are_owned_by_the_successor(self):
        graph = _revision_graph()

        # One recorder plays the one database both migrations run on:
        # the applied journal revision first, the successor after it.
        recorder = _OpRecorder()
        graph[MIGRATION_REVISION]['module'].op = recorder
        graph[MIGRATION_REVISION]['module'].upgrade()
        base_columns = sorted(
            col.name
            for col in recorder.tables['agent_channel_commands'].c)
        self.assertEqual(
            base_columns, sorted(ORIGINAL_COMMAND_COLUMNS),
            'the applied revision %s must create '
            'agent_channel_commands without the report slots %s; '
            'found %s'
            % (MIGRATION_REVISION, sorted(REPORT_SLOTS), base_columns))

        head = self._assert_single_successor(graph)
        graph[head]['module'].op = recorder
        graph[head]['module'].upgrade()
        added = sorted(recorder.added_columns.get(
            'agent_channel_commands', []))
        self.assertEqual(
            added, sorted(REPORT_SLOTS),
            'the successor revision %s must add exactly the report '
            'slots %s to agent_channel_commands, found %s'
            % (head, sorted(REPORT_SLOTS), added))
        self.assertEqual(
            set(recorder.added_columns),
            {'agent_channel_commands'},
            'the successor revision %s must add columns to '
            'agent_channel_commands only, found %s'
            % (head, sorted(recorder.added_columns)))

    def test_the_successor_downgrade_drops_the_report_slots(self):
        graph = _revision_graph()
        head = self._assert_single_successor(graph)

        # The downgrade runs on a database at the journal head.
        recorder = _run_migrations(graph, _journal_chain(graph))
        head_module = graph[head]['module']
        head_module.op = recorder
        head_module.downgrade()
        dropped = sorted(recorder.dropped_columns.get(
            'agent_channel_commands', []))
        self.assertEqual(
            dropped, sorted(REPORT_SLOTS),
            'the successor revision %s must drop the report slots %s '
            'from agent_channel_commands in downgrade(), found %s'
            % (head, sorted(REPORT_SLOTS), dropped))
