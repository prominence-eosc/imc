FROM alpine:latest
RUN apk add --no-cache gcc \
                       musl-dev \
                       linux-headers \
                       uwsgi-python \
                       uwsgi-http \
                       python \
                       python-dev \
                       py-requests \ 
                       py-flask \
                       py-futures \
                       py-paramiko \
                       py-psycopg2 \
                       py-pip && \
    pip install apache-libcloud && \
    pip install python-openstackclient


COPY imc /imc

# LibCloud fix
COPY openstack_identity.py /usr/lib/python2.7/site-packages/libcloud/common/

RUN chown uwsgi /var/log && \
    mkdir /var/lib/prominence && \
    chown uwsgi /var/lib/prominence

ENTRYPOINT ["/usr/sbin/uwsgi", \
            "--plugins-dir", "/usr/lib/uwsgi", \
            "--plugins", "http,python", \
            "--http-socket", ":5000", \
            "--threads", "2", \
            "--uid", "uwsgi", \
            "--manage-script-name", \
            "--master", \
            "--chdir", "/imc", \
            "--mount", "/=restapi:app"]
