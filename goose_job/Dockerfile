# goose job microVM image. Build it into the rootless Podman storage:
#   ./scripts/build-goose-image.sh
#
# The image intentionally contains no curl, wget, or other network clients in
# its final stage. The only outbound path is the Kafkallm LLM gateway used by
# goose; the provider credentials stay in the gateway.
FROM docker.io/library/debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS goose-download

ARG GOOSE_VERSION=1.59.0
ARG GOOSE_SHA256=a774667de6c20206a60d78d7bece2d9dda8389667e4f5c2fc8aaa758d1e81ef3

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/goose.tar.gz \
      "https://github.com/aaif-goose/goose/releases/download/v${GOOSE_VERSION}/goose-x86_64-unknown-linux-gnu.tar.gz" \
    && echo "${GOOSE_SHA256}  /tmp/goose.tar.gz" | sha256sum -c - \
    && mkdir -p /opt/goose \
    && tar -xzf /tmp/goose.tar.gz -C /opt/goose \
    && install -D -m 0755 /opt/goose/goose /usr/local/goose/bin/goose

FROM docker.io/library/debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

# Add extra Debian packages here or at build time, for example:
#   GOOSE_JOB_EXTRA_PACKAGES="openjdk-17-jre-headless" ./scripts/build-goose-image.sh
ARG GOOSE_JOB_EXTRA_PACKAGES=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        binutils \
        ca-certificates \
        coreutils \
        file \
        findutils \
        git \
        jq \
        less \
        procps \
        python3 \
        ripgrep \
        unzip \
        xxd \
        zip \
        ${GOOSE_JOB_EXTRA_PACKAGES} \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /workspace /opt/data /opt/goosetown /opt/kafkallm

COPY --from=goose-download /usr/local/goose/bin/goose /usr/local/goose/bin/goose
COPY goose_job/template /opt/goosetown/template
COPY goose_job/skills /opt/goosetown/skills
COPY goose_job/LICENSE-goosetown /opt/goosetown/LICENSE
COPY goose_job/gateway_proxy.py /opt/kafkallm/gateway_proxy.py

ENV GOOSE_BIN=/usr/local/goose/bin/goose \
    PATH=/usr/local/goose/bin:$PATH \
    GOOSE_TELEMETRY_ENABLED=false \
    GOOSE_DISABLE_KEYRING=1 \
    HOME=/opt/data

WORKDIR /workspace

CMD ["/bin/sh", "-c", "exec /bin/sh"]
