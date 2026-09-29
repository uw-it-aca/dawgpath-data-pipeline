ARG DJANGO_CONTAINER_VERSION=3.1.4

FROM us-docker.pkg.dev/uwit-mci-axdd/containers/django-container:${DJANGO_CONTAINER_VERSION} AS app-prebundler-container

USER root

RUN apt-get update && apt-get install -y libpq-dev curl gnupg

# Azure SQL (StudentAnalytics) needs Entra ID password auth, which pymssql cannot do.
RUN . /etc/os-release && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/${ID}/${VERSION_ID}/prod ${VERSION_CODENAME} main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

USER acait

ADD --chown=acait:acait . /app/
ADD --chown=acait:acait docker/ /app/project/

ADD --chown=acait:acait docker/app_start.sh /scripts
ADD --chown=acait:acait docker/app_deploy.sh /scripts
RUN chmod u+x /scripts/app_start.sh /scripts/app_deploy.sh

RUN /app/bin/pip install -r requirements.txt
RUN /app/bin/pip install psycopg2 dagster dagster-webserver google-cloud-storage

FROM app-prebundler-container AS app-container

RUN /app/bin/python manage.py collectstatic --noinput

FROM us-docker.pkg.dev/uwit-mci-axdd/containers/django-test-container:${DJANGO_CONTAINER_VERSION} AS app-test-container

ENV NODE_PATH=/app/lib/node_modules
COPY --from=app-container /app/ /app/
COPY --from=app-container /static/ /static/
