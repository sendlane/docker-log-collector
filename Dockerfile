FROM python:3.6-stretch

RUN set -ex; \
	apt-get update; \
	apt-get install --no-install-recommends -y \
		lighttpd \
		supervisor \
		cron \
		rsyslog \
		telnet \
		netcat \
	; \
	( apt-get clean && rm -rf /var/lib/apt/lists/* )

ENV PYTHON_PIP_VERSION 10.0.1
RUN set -ex; \
    wget -O get-pip.py 'https://bootstrap.pypa.io/get-pip.py';  \
    python3 get-pip.py \
        --disable-pip-version-check \
        --no-cache-dir \
        "pip==$PYTHON_PIP_VERSION" \
    ; \
    pip --version; \
    \
    find /usr/local -depth \
        \( \
            \( -type d -a \( -name test -o -name tests \) \) \
            -o \
            \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
        \) -exec rm -rf '{}' +; \
    rm -f get-pip.py;

# Setting env variable to avoid one of Airflow's dependencies to install a GPL dependency (unidecode)
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
RUN set -ex; \
    pip install python-dotenv j2cli
RUN set -ex; \
    pip install requests

RUN rm /etc/localtime
RUN ln -s /usr/share/zoneinfo/US/Pacific /etc/localtime

ADD /files/nsq-1.1.0.linux-amd64.go1.10.3.tar.gz /tmp
RUN cp /tmp/nsq-1.1.0.linux-amd64.go1.10.3/bin/* /usr/local/bin/

ADD files/lighttpd.conf /etc/lighttpd/lighttpd.conf
ADD start /start
ADD files/logger.conf /tmp/logger.conf
RUN chmod ug+x /start
ADD files/monitor_log_channels.py /code/scripts/monitor_log_channels.py
RUN chmod ug+x /code/scripts/monitor_log_channels.py
CMD /start
