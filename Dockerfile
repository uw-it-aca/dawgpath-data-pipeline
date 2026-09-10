ARG DJANGO_CONTAINER_VERSION=3.1.4

FROM us-docker.pkg.dev/uwit-mci-axdd/containers/django-container:${DJANGO_CONTAINER_VERSION} AS app-prebundler-container

USER root

RUN apt-get update && apt-get install -y libpq-dev

USER acait

ADD --chown=acait:acait . /app/
ADD --chown=acait:acait docker/ /app/project/

ADD --chown=acait:acait docker/app_start.sh /scripts
ADD --chown=acait:acait docker/app_deploy.sh /scripts
RUN chmod u+x /scripts/app_start.sh /scripts/app_deploy.sh

RUN /app/bin/pip install -r requirements.txt
RUN /app/bin/pip install psycopg2 dagster dagster-webserver google-cloud-storage

FROM node:lts-bullseye AS node-bundler

ADD ./package.json /app/
WORKDIR /app/
RUN npm install .

ADD . /app/

ARG VUE_DEVTOOLS
ENV VUE_DEVTOOLS=$VUE_DEVTOOLS
RUN npm run build

FROM app-prebundler-container AS app-container

COPY --chown=acait:acait --from=node-bundler /app/dawgpath_pipeline_admin/static /app/dawgpath_pipeline_admin/static

RUN /app/bin/python manage.py collectstatic --noinput

FROM us-docker.pkg.dev/uwit-mci-axdd/containers/django-test-container:${DJANGO_CONTAINER_VERSION} AS app-test-container

ENV NODE_PATH=/app/lib/node_modules
COPY --from=app-container /app/ /app/
COPY --from=app-container /static/ /static/
