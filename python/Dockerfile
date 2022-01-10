FROM ubuntu:latest
ARG DEBIAN_FRONTEND=noninteractive
ENV AWS_ACCESS_KEY_ID="AWS_ACCESS_KEY_ID"
ENV AWS_SECRET_ACCESS_KEY="AWS_SECRET_ACCESS_KEY"

RUN apt-get update && apt-get install -y python3-pip software-properties-common && apt-get update

# Install Python Setuptools
RUN apt-get install -y python3 cron python3-bs4

RUN apt-get purge -y software-properties-common && apt-get clean -y && apt-get autoclean -y && apt-get autoremove -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Setup cron job
RUN (crontab -l ; echo "*/10 * * * * python3 -u /python/app.py >> /var/log/cron.log 2>&1") | crontab

COPY ./ ./python/
COPY ./tests/ ./tests/

RUN pip3 install -r python/requeriments.txt
WORKDIR python/

CMD sleep 5 && python3 -u /python/app.py && cron && tail -f /var/log/cron.log

