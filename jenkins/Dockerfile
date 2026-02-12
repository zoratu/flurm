FROM jenkins/jenkins:lts-jdk17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       git \
       make \
       erlang \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/erlang/rebar3/releases/download/3.25.1/rebar3 \
      -o /usr/local/bin/rebar3 \
    && chmod +x /usr/local/bin/rebar3

COPY jenkins/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

USER jenkins
