source "/app/bin/activate"

# Pipeline deploy
cd /app/dawgpath_data_pipeline
alembic upgrade head

# Django deploy
cd /app
python manage.py migrate
