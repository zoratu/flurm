# SLURM Docker Image for Migration Testing
# Based on Debian slim for smaller size while maintaining compatibility

FROM debian:bookworm-slim

LABEL maintainer="FLURM Project"
LABEL description="Minimal SLURM installation for migration testing"

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies and SLURM
RUN apt-get update && apt-get install -y --no-install-recommends \
    slurm-wlm \
    slurm-client \
    slurmd \
    slurmctld \
    munge \
    libmunge-dev \
    procps \
    iproute2 \
    netcat-openbsd \
    curl \
    vim-tiny \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /var/spool/slurm/ctld \
    /var/spool/slurm/d \
    /var/log/slurm \
    /var/run/slurm \
    /etc/slurm \
    && chown -R slurm:slurm /var/spool/slurm \
    && chown -R slurm:slurm /var/log/slurm \
    && chown -R slurm:slurm /var/run/slurm

# Copy SLURM configuration
COPY config/slurm.conf /etc/slurm/slurm.conf
COPY config/cgroup.conf /etc/slurm/cgroup.conf

# Copy munge key (for authentication)
COPY config/munge.key /etc/munge/munge.key
RUN chmod 400 /etc/munge/munge.key \
    && chown munge:munge /etc/munge/munge.key

# Copy startup scripts
COPY scripts/start-slurm-controller.sh /usr/local/bin/start-slurm-controller.sh
COPY scripts/start-slurm-node.sh /usr/local/bin/start-slurm-node.sh
COPY scripts/submit-test-jobs.sh /usr/local/bin/submit-test-jobs.sh
RUN chmod +x /usr/local/bin/*.sh

# Expose SLURM ports
# slurmctld: 6817
# slurmd: 6818
# srun: 6819
EXPOSE 6817 6818 6819

# Default command (will be overridden in docker-compose)
CMD ["/usr/local/bin/start-slurm-controller.sh"]
