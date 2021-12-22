"""update major credential desc

Revision ID: 140915d317a6
Revises: 8cf3c0cf9e93
Create Date: 2021-12-17 23:58:35.199438+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '140915d317a6'
down_revision = '8cf3c0cf9e93'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('major', 'credential_description',
               existing_type=sa.VARCHAR(length=25),
               type_=sa.String(length=2500),
               existing_nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('major', 'credential_description',
               existing_type=sa.String(length=2500),
               type_=sa.VARCHAR(length=25),
               existing_nullable=True)
    # ### end Alembic commands ###
