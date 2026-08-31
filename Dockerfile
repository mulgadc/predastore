# Predastore — S3-compatible object storage.
#
#   docker build -t predastore:dev .
#   docker run --rm -p 8443:8443 predastore:dev
#
# The second command is the whole quick start: with no peers configured the
# entrypoint renders a single-host profile, generates a TLS identity and an
# at-rest key into the data volume, and serves S3 on 8443.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS builder

# GOFIPS140 is a build-time contract, not a preference. bluebottle/pkg/fipsboot
# is a blank import in cmd/s3d, and its init() panics unless the binary was
# built in FIPS mode. Setting it here rather than on the go build line means a
# later stage cannot quietly drop it.
ENV GOFIPS140=v1.0.0 \
    CGO_ENABLED=0

WORKDIR /build

# predastore is a self-contained module with no replace directives, so the
# dependency layer caches on go.mod/go.sum alone.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -ldflags "-s -w" -o /out/s3d ./cmd/s3d

# Prove the binary starts before it is ever shipped. A FIPS-less build compiles
# cleanly and then panics in init(), so the failure would otherwise surface as a
# crash-looping container rather than a failed build. This has happened before.
RUN out="$(/out/s3d -h 2>&1 || true)"; \
    case "$out" in \
      *"FIPS 140-3 mode is not enabled"*) \
        echo "FIPS guard: s3d was built without GOFIPS140" >&2; exit 1 ;; \
    esac; \
    case "$out" in \
      *"-admin-port"*) : ;; \
      *) echo "FIPS guard: s3d did not reach flag parsing:" >&2; \
         echo "$out" >&2; exit 1 ;; \
    esac


FROM debian:trixie-slim AS runtime

# openssl generates the TLS identity and the at-rest key on first start; curl
# backs the healthcheck below. Both could go once s3d grows subcommands for
# them, which is what a distroless runtime stage would need.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl curl \
    && rm -rf /var/lib/apt/lists/*

# A fixed uid so a volume's ownership survives an image rebuild, and so a
# Kubernetes securityContext can name the same number.
ARG PREDA_UID=10001
ARG PREDA_GID=10001
RUN groupadd --gid "${PREDA_GID}" predastore \
    && useradd --uid "${PREDA_UID}" --gid "${PREDA_GID}" \
        --home-dir /var/lib/predastore --no-create-home --shell /usr/sbin/nologin predastore

COPY --from=builder /out/s3d /usr/local/bin/s3d
COPY --from=builder /build/config/ /etc/predastore/config/
COPY deploy/docker/entrypoint.sh deploy/docker/render-config.sh /usr/local/bin/

RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/render-config.sh \
    && mkdir -p /var/lib/predastore /etc/predastore/rendered \
    && chown "${PREDA_UID}:${PREDA_GID}" /var/lib/predastore /etc/predastore/rendered

# No TLS material is baked in. A key inside a published image is a public key,
# and every deployment that pulled the image would share it.

# S3 gate, meta/raft, the first blob node, and the admin listener.
EXPOSE 8443 6660 9991 9099

VOLUME ["/var/lib/predastore"]

USER predastore

# readyz answers from a background sampler, so probing it is an atomic load
# rather than work. start-period covers a multi-host cluster electing a leader.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${PREDA_ADMIN_PORT:-9099}/readyz" >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
