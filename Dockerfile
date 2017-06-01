FROM bitnami/minideb:jessie

ARG REPLIGATOR_VERSION=0.1.0
ARG VERTICA_DRIVER_ARCH_LINK=""

RUN apt-get update && \
    apt-get install -y ca-certificates wget unixodbc unixodbc-dev && \
    wget "https://github.com/b13f/repligator/releases/download/v${REPLIGATOR_VERSION}/repligator-linux-amd64.tar.gz" -O /tmp/repligator.tar.gz && \
    tar -xvf /tmp/repligator.tar.gz -C /bin && \
    wget "${VERTICA_DRIVER_ARCH_LINK}" -O /tmp/vertica-client.tar.gz && \
    tar -xvf /tmp/vertica-client.tar.gz -C / opt/vertica/en-US opt/vertica/lib64 opt/vertica/include && \
    printf "[Vertica]\nDescription = Vertica driver\nDriver = /opt/vertica/lib64/libverticaodbc.so" | odbcinst -i -d -r && \
    printf "[Vertica]\nDescription = Vertica\nDriver = /opt/vertica/lib64/libverticaodbc.so\nDriver = Vertica" | odbcinst -i -s -r && \
    rm -rf /tmp/* && apt-get remove -y --auto-remove ca-certificates wget

VOLUME /etc/repligator.yml

ENTRYPOINT ["repligator"]

CMD ["-config","/etc/repligator.yml"]

EXPOSE 8080
