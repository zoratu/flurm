# FLURM Dockerfile
# Multi-stage build for all FLURM components

# Stage 1: Builder
FROM erlang:26-alpine AS builder

RUN apk add --no-cache git make gcc musl-dev

WORKDIR /build

# Copy source
COPY . .

# Get dependencies and compile all releases
RUN rebar3 as prod release -n flurmctld && \
    rebar3 as prod release -n flurmnd && \
    rebar3 as prod release -n flurmbd

# Stage 2: Controller image
FROM erlang:26-alpine AS flurmctld

RUN apk add --no-cache openssl ncurses-libs libstdc++

WORKDIR /app

COPY --from=builder /build/_build/prod/rel/flurmctld ./

# Create directories
RUN mkdir -p /var/lib/flurm /var/log/flurm /etc/flurm

# Set entrypoint
ENTRYPOINT ["/app/bin/flurmctld"]
CMD ["foreground"]

EXPOSE 6817 4369 9090

# Stage 3: Node daemon image
FROM erlang:26-alpine AS flurmnd

RUN apk add --no-cache openssl ncurses-libs libstdc++ coreutils

WORKDIR /app

COPY --from=builder /build/_build/prod/rel/flurmnd ./

# Create directories
RUN mkdir -p /var/lib/flurm /var/log/flurm /etc/flurm

# Set entrypoint
ENTRYPOINT ["/app/bin/flurmnd"]
CMD ["foreground"]

EXPOSE 6818

# Stage 4: DBD image
FROM erlang:26-alpine AS flurmbd

RUN apk add --no-cache openssl ncurses-libs libstdc++

WORKDIR /app

COPY --from=builder /build/_build/prod/rel/flurmbd ./

# Create directories
RUN mkdir -p /var/lib/flurm/dbd /var/log/flurm /etc/flurm

# Set entrypoint
ENTRYPOINT ["/app/bin/flurmbd"]
CMD ["foreground"]

EXPOSE 6819
