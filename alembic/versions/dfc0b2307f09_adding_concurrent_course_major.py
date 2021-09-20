"""adding concurrent course major

Revision ID: dfc0b2307f09
Revises: 3ea506050525
Create Date: 2021-08-11 00:10:23.681256+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dfc0b2307f09'
down_revision = '3ea506050525'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('concurrentcoursesmajor',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('major_id', sa.String(length=6), nullable=True),
    sa.Column('concurrent_courses', sa.PickleType(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('major_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('concurrentcoursesmajor')
    # ### end Alembic commands ###