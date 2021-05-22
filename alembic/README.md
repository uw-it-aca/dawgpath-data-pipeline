Generic single-database configuration.



Create migrations:
Alembic bases migrations from the existing database state, 
thus to create a migration you should start with a fresh database, apply
existing migrations, then generate a migration for your model changes.

Note: New models must be imported in env.py for autogenerate to pick them up


```alembic revision -m "[revision name]" --autogenerate```

Apply migrations
```alembic upgrade head```
