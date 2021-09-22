FROM golang:1.16.8 AS build

WORKDIR /src
# enable modules caching in separate layer
COPY go.mod go.sum ./
RUN go mod download
COPY . ./

RUN make binary

FROM debian:10.2-slim

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd -r aurorafs --gid 999; \
    useradd -r -g aurorafs --uid 999 --no-log-init -m aurorafs;

# make sure mounted volumes have correct permissions
RUN mkdir -p /home/aurorafs/.aurorafs && chown 999:999 /home/aurorafs/.aurorafs

COPY --from=build /src/dist/aurorafs /usr/local/bin/aurorafs

EXPOSE 1633 1634 1635
USER aurorafs
WORKDIR /home/aurorafs
VOLUME /home/aurorafs/.aurorafs

ENTRYPOINT ["aurorafs"]
