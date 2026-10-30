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

"""Add the agent channel journal tables

Revision ID: dfb37433ec66
Revises: 9fb44677ef15
Create Date: 2026-10-30 00:00:00.000000

"""

from alembic import op
from oslo_db.sqlalchemy import types as db_types
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'dfb37433ec66'
down_revision = '9fb44677ef15'


def upgrade():
    op.create_table(
        'agent_channel_heads',
        sa.Column('version', sa.String(length=15), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('node_id', sa.Integer(), nullable=False),
        sa.Column('workflow_epoch', sa.Integer(), nullable=False),
        sa.Column('session_uuid', sa.String(length=36), nullable=True),
        sa.Column('owner_conductor_id', sa.Integer(), nullable=True),
        sa.Column('owner_incarnation', sa.String(length=255), nullable=True),
        sa.Column('fence', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['node_id'], ['nodes.id']),
        sa.ForeignKeyConstraint(['owner_conductor_id'], ['conductors.id']),
        sa.UniqueConstraint('node_id',
                            name='uniq_agent_channel_heads0node_id'),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4'
    )

    op.create_table(
        'agent_channel_sessions',
        sa.Column('version', sa.String(length=15), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('uuid', sa.String(length=36), nullable=False),
        sa.Column('node_id', sa.Integer(), nullable=False),
        sa.Column('execution_id', sa.String(length=36), nullable=False),
        sa.Column('workflow_epoch', sa.Integer(), nullable=False),
        sa.Column('fence', sa.Integer(), nullable=False),
        sa.Column('owner_conductor_id', sa.Integer(), nullable=True),
        sa.Column('owner_incarnation', sa.String(length=255), nullable=True),
        sa.Column('token_verifier', sa.String(length=255), nullable=False),
        sa.Column('profile', sa.String(length=255), nullable=False),
        sa.Column('state', sa.String(length=16), nullable=False),
        sa.Column('next_sequence', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['node_id'], ['nodes.id']),
        sa.ForeignKeyConstraint(['owner_conductor_id'], ['conductors.id']),
        sa.UniqueConstraint('uuid',
                            name='uniq_agent_channel_sessions0uuid'),
        sa.UniqueConstraint('node_id', 'execution_id',
                            name='uniq_agent_channel_sessions0node_exec'),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4'
    )
    op.create_index('agent_channel_sessions_node_idx',
                    'agent_channel_sessions', ['node_id'])

    op.create_table(
        'agent_channel_commands',
        sa.Column('version', sa.String(length=15), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('uuid', sa.String(length=36), nullable=False),
        sa.Column('session_uuid', sa.String(length=36), nullable=False),
        sa.Column('workflow_epoch', sa.Integer(), nullable=False),
        sa.Column('invocation_key', sa.String(length=255), nullable=False),
        sa.Column('sequence', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        # NOTE: Command parameters may exceed the MySQL TEXT limit, so the
        # column must use the LONGTEXT variant the model declares.
        sa.Column('params',
                  db_types.JsonEncodedDict(mysql_as_long=True).impl,
                  nullable=False),
        sa.Column('reply_boundary', sa.String(length=32), nullable=False),
        sa.Column('params_sha256', sa.String(length=64), nullable=False),
        sa.Column('state', sa.String(length=16), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['session_uuid'],
                                ['agent_channel_sessions.uuid']),
        sa.UniqueConstraint('uuid',
                            name='uniq_agent_channel_commands0uuid'),
        sa.UniqueConstraint('session_uuid', 'workflow_epoch',
                            'invocation_key',
                            name='uniq_agent_channel_commands0identity'),
        sa.UniqueConstraint('session_uuid', 'sequence',
                            name='uniq_agent_channel_commands0sequence'),
        mysql_engine='InnoDB',
        mysql_charset='utf8mb4'
    )
    op.create_index('agent_channel_commands_session_idx',
                    'agent_channel_commands', ['session_uuid'])


def downgrade():
    op.drop_index('agent_channel_commands_session_idx',
                  table_name='agent_channel_commands')
    op.drop_table('agent_channel_commands')
    op.drop_index('agent_channel_sessions_node_idx',
                  table_name='agent_channel_sessions')
    op.drop_table('agent_channel_sessions')
    op.drop_table('agent_channel_heads')
