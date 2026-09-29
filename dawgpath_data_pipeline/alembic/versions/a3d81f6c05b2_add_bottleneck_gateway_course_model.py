# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""add bottleneck gateway course model

Revision ID: a3d81f6c05b2
Revises: b7f2e9c1a4d3
Create Date: 2026-09-28 00:00:00.000000+00:00

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a3d81f6c05b2'
down_revision = 'b7f2e9c1a4d3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bottleneckgatewaycourse',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('department_abbrev', sa.String(length=20), nullable=True),
        sa.Column('course_number', sa.SmallInteger(), nullable=True),
        sa.Column('course_branch', sa.SmallInteger(), nullable=True),
        sa.Column('is_bottleneck', sa.Boolean(), nullable=True),
        sa.Column('is_gateway', sa.Boolean(), nullable=True),
        sa.Column('bottleneck_severity', sa.Float(), nullable=True),
        sa.Column('gateway_significance', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table('bottleneckgatewaycourse')
