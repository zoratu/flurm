# Jenkins (Docker) Setup

This project includes a Dockerized Jenkins controller and a repo `Jenkinsfile` that runs the same consistency checks as local hooks.

## What It Runs

- `make check-quick`
- `make check-prepush`

`check-prepush` includes:
- `rebar3 as test eunit --app=flurm_dbd --cover`
- coverage threshold gate for:
  - `flurm_dbd_fragment`
  - `flurm_dbd_acceptor`
  - `flurm_dbd_storage`
  - `flurm_dbd_mysql`
  - `flurm_dbd_server`

## Start Jenkins

From repo root:

```bash
docker compose -f docker/docker-compose.jenkins.yml up -d --build
```

Get initial admin password:

```bash
docker exec flurm-jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Open:

- [http://localhost:8080](http://localhost:8080)

## Create Pipeline Job (Clone Preferred)

1. New Item -> Pipeline
2. Under **Pipeline**, choose **Pipeline script from SCM**
3. SCM: **Git**
4. Repository URL: your FLURM repo URL
5. Branch Specifier: your target branch (for example `*/main`)
6. Script Path: `Jenkinsfile`
7. Save and Build Now

## Notes

- Jenkins container includes `erlang`, `rebar3`, `git`, and `make`.
- Docker socket is mounted, so Docker-based checks can be enabled later.
- To run full checks with Docker interop, set env var in Jenkins job:

```bash
FLURM_CHECK_DOCKER=1
```
