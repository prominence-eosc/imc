FROM alpine:latest
RUN apk add --no-cache uwsgi-python \
                       uwsgi-http \
                       python \
                       py-requests \ 
                       py-flask \
                       py-futures \
                       py-paramiko
    

COPY restapi.py /
COPY imc.py /
COPY database.py /
COPY imclient.py /
COPY opaclient.py /
COPY tokens.py /
COPY utilities.py /

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
            "--mount", "/=restapi:app"]
