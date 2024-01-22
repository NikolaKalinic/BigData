FROM apache/superset:latest

USER root

RUN pip install pip install psycopg2

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD


COPY superset-config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset

CMD superset fab create-admin --username $ADMIN_USERNAME --firstname Superset --lastname Admin --email $ADMIN_EMAIL --password $ADMIN_PASSWORD && superset db upgrade && superset superset init && superset set-database-uri --uri "$HIVE_CONNECTION" --database-name "Hive" &&/usr/bin/run-server.sh