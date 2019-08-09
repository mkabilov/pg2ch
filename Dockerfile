ARG PG_VERSION
FROM postgres:${PG_VERSION}-alpine

RUN echo 'http://dl-3.alpinelinux.org/alpine/edge/main' > /etc/apk/repositories; \
    echo 'http://dl-3.alpinelinux.org/alpine/edge/community' >> /etc/apk/repositories; \
	apk --no-cache add git go musl-dev;

ENV LANG=C.UTF-8 PGDATA=/pg/data
ENV SRC="/pg/go/src/github.com/mkabilov/pg2ch"

RUN mkdir -p ${PGDATA} && \
	mkdir -p ${SRC} && \
	chown postgres:postgres ${PGDATA} && \
	chmod a+rwx /usr/local/lib/postgresql && \
	chmod a+rwx /usr/local/share/postgresql/extension && \
	mkdir -p /usr/local/share/doc/postgresql/contrib && \
	chmod a+rwx /usr/local/share/doc/postgresql/contrib

ADD . ${SRC}
WORKDIR ${SRC}
RUN chmod -R go+rwX /pg/go
USER postgres
ENTRYPOINT PGDATA=${PGDATA} GOPATH="/pg/go" bash run_tests.sh
