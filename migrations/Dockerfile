FROM flyway/flyway

USER root

COPY . /flyway/

ENTRYPOINT ["/bin/bash", "/flyway/migrations.sh"]