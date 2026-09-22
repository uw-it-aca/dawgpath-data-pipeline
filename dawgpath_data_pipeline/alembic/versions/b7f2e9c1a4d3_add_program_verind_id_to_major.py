# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""add program version id to major

Revision ID: b7f2e9c1a4d3
Revises: 4d49ff73e1e6
Create Date: 2026-09-22 00:00:00.000000+00:00

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b7f2e9c1a4d3'
down_revision = '4d49ff73e1e6'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('major', sa.Column('program_verind_id', sa.String(length=50), nullable=True))


def downgrade():
    op.drop_column('major', 'program_verind_id')