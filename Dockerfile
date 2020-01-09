FROM alpine:latest
RUN apk add --no-cache gcc \
                       musl-dev \
                       linux-headers \
                       uwsgi-python \
                       uwsgi-http \
                       python3 \
                       python3-dev \
                       py3-requests \ 
                       py3-flask \
                       py3-future \
                       py3-paramiko \
                       py3-psycopg2 \
                       py3-pip && \
    pip3 install apache-libcloud && \
    pip3 install python-openstackclient


COPY imc /imc

RUN chown uwsgi /var/log && \
    mkdir /var/lib/prominence && \
    chown uwsgi /var/lib/prominence

ENTRYPOINT ["/usr/sbin/uwsgi", \
            "--plugins-dir", "/usr/lib/uwsgi", \
            "--plugins", "http,python", \
            "--http-socket", "127.0.0.1:5000", \
            "--threads", "2", \
            "--uid", "uwsgi", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/imc", \
            "--mount", "/=restapi:app"]
