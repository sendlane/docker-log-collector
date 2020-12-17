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
        vim \
        jq \
	; \
	( apt-get clean && rm -rf /var/lib/apt/lists/* )

ENV PYTHON_PIP_VERSION 20.1
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

ADD files/requirements.txt /tmp/requirements.txt
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
RUN set -ex; \
    pip install -r /tmp/requirements.txt

RUN rm /etc/localtime
RUN ln -s /usr/share/zoneinfo/US/Eastern /etc/localtime

ADD files/nsq-1.1.0.linux-amd64.go1.10.3.tar.gz /tmp
RUN cp /tmp/nsq-1.1.0.linux-amd64.go1.10.3/bin/* /usr/local/bin/

ADD start /start
ADD files/logger.conf /tmp/logger.conf
RUN chmod ug+x /start
COPY files/50-default.conf /etc/rsyslog.d/50-default.conf
COPY files/vimrc /root/.vimrc
COPY files/root-crontab /var/spool/cron/crontabs/root
RUN chown root:crontab /var/spool/cron/crontabs/root
RUN chmod 0600 /var/spool/cron/crontabs/root
CMD /start
