"""adding index to registration

Revision ID: 8709db335246
Revises: 7ef08ed98333
Create Date: 2021-08-05 23:44:38.884358+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8709db335246'
down_revision = '7ef08ed98333'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(op.f('ix_registration_regis_term'), 'registration', ['regis_term'], unique=False)
    op.create_index(op.f('ix_registration_system_key'), 'registration', ['system_key'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_registration_system_key'), table_name='registration')
    op.drop_index(op.f('ix_registration_regis_term'), table_name='registration')
    # ### end Alembic commands ###
