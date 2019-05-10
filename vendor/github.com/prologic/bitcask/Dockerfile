# Build
FROM prologic/go-builder:latest AS build

# Runtime
FROM alpine

COPY --from=build /src/bitcaskd /bitcaskd

EXPOSE 6379/tcp

VOLUME /data

ENTRYPOINT ["/bitcaskd"]
CMD ["/data"]
