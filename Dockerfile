FROM alpine:latest
RUN apk add --no-cache uwsgi-python \
                       uwsgi-http \
                       python \
                       py-requests \
                       py-flask \
                       py-futures \
                       py-paramiko


COPY imc.py /
COPY imclient.py /
RUN chown uwsgi /var/log && \
    mkdir /var/lib/prominence && \
    chown uwsgi /var/lib/prominence

ENTRYPOINT ["/usr/sbin/uwsgi", \
            "--plugins-dir", "/usr/lib/uwsgi", \
            "--plugins", "http,python", \
            "--http-socket", ":5000", \
            "--uid", "uwsgi", \
            "--manage-script-name", \
            "--master", \
            "--mount", "/=imc:app"]
