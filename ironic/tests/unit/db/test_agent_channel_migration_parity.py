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

"""Unit test pinning the type parity of the agent channel journal DDL.

The DB unit tests build the SQLite schema from ``models`` and never
run the alembic migration, so the migration and the models can drift
on a column type that only bites on the backends the unit tests do
not exercise (for example TEXT vs LONGTEXT on MySQL).

This test executes the upgrade() of the agent channel journal
migration (revision dfb37433ec66) against a stubbed alembic ``op``
that records the ``sa.Table`` objects built by ``op.create_table``,
and compares the DDL type of every column of the three journal
tables between the migration and ``models.Base.metadata``, compiled
against the MySQL and PostgreSQL dialects.
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

# The dialects the unit tests never exercise through a live schema.
DIALECTS = (mysql.dialect(), postgresql.dialect())


class _OpRecorder(object):
    """A stand-in for alembic ``op`` that records created tables.

    ``create_table`` builds the same ``sa.Table`` that alembic's
    ``op.create_table`` builds from the same arguments, so the
    recorded table keeps the column types the migration declares.
    """

    def __init__(self):
        self.metadata = sa.MetaData()
        self.tables = {}

    def create_table(self, table_name, *columns, **kwargs):
        self.tables[table_name] = sa.Table(
            table_name, self.metadata, *columns, **kwargs)

    def create_index(self, index_name, table_name, columns, **kwargs):
        # Index placement is not a type-parity concern.
        pass

    def __getattr__(self, name):
        raise AttributeError(
            'the type-parity check records only create_table; the '
            'migration used op.%s' % name)


def _load_migration():
    """Import the journal migration module by its revision id."""
    versions_dir = os.path.join(os.path.dirname(models.__file__),
                                'alembic', 'versions')
    for name in sorted(os.listdir(versions_dir)):
        if name.startswith(MIGRATION_REVISION + '_') and \
                name.endswith('.py'):
            path = os.path.join(versions_dir, name)
            spec = importlib.util.spec_from_file_location(name[:-3], path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise AssertionError(
        'migration %s not found under %s' % (MIGRATION_REVISION,
                                             versions_dir))


def _migration_tables():
    """The tables the migration creates, with its declared types.

    Runs the migration's upgrade() against a stubbed alembic ``op``
    that records the ``sa.Table`` objects of the create_table calls.
    """
    migration = _load_migration()
    recorder = _OpRecorder()
    migration.op = recorder
    migration.upgrade()
    return recorder.tables


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

    def test_migration_column_types_match_the_models(self):
        migration_tables = _migration_tables()
        for table_name in AGENT_CHANNEL_TABLES:
            self.assertIn(
                table_name, migration_tables,
                'the journal migration creates no table %s' % table_name)
            self._assert_column_types_match(
                table_name, migration_tables[table_name])
