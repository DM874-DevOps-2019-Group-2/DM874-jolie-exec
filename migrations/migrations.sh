#!/bin/bash

echo "flyway.url=jdbc:postgresql://"$DATABASE_HOST":"$DATABASE_PORT"/jolie_exec_db" > /flyway/conf/flyway.conf
echo "flyway.user="$POSTGRES_USER >> /flyway/conf/flyway.conf
echo "flyway.password="$POSTGRES_PASSWORD >> /flyway/conf/flyway.conf
echo "flyway.baselineOnMigrate=true" >> /flyway/conf/flyway.conf

bash /flyway/flyway -configFiles=/flyway/conf/flyway.conf -locations=filesystem:/flyway/scripts/ migrate