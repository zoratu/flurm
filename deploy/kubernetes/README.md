# FLURM Kubernetes Deployment

This directory contains Kubernetes manifests for deploying FLURM.

## Prerequisites

- Kubernetes 1.25+
- kubectl configured
- Storage class available (default: `standard`)
- Container registry access

## Quick Start

### 1. Build and Push Images

```bash
# Build images
docker build --target flurmctld -t your-registry/flurm/flurmctld:latest .
docker build --target flurmnd -t your-registry/flurm/flurmnd:latest .
docker build --target flurmbd -t your-registry/flurm/flurmbd:latest .

# Push to registry
docker push your-registry/flurm/flurmctld:latest
docker push your-registry/flurm/flurmnd:latest
docker push your-registry/flurm/flurmbd:latest
```

### 2. Update Image References

Edit `kustomization.yaml` to use your registry:

```yaml
images:
- name: flurm/flurmctld
  newName: your-registry/flurm/flurmctld
  newTag: latest
```

### 3. Update Secrets

Edit `config.yaml` to set a secure Erlang cookie:

```yaml
stringData:
  erlang-cookie: "YOUR_SECURE_COOKIE_HERE"
```

### 4. Deploy

```bash
# Apply all resources
kubectl apply -k .

# Or apply individually
kubectl apply -f namespace.yaml
kubectl apply -f config.yaml
kubectl apply -f controller-statefulset.yaml
kubectl apply -f dbd-deployment.yaml
kubectl apply -f node-daemonset.yaml
```

### 5. Label Compute Nodes

```bash
# Mark nodes as FLURM compute nodes
kubectl label node node1 flurm.io/compute-node=true
kubectl label node node2 flurm.io/compute-node=true
```

## Verification

```bash
# Check controller pods
kubectl get pods -n flurm -l app.kubernetes.io/component=controller

# Check controller logs
kubectl logs -n flurm flurmctld-0

# Check cluster status
kubectl exec -n flurm flurmctld-0 -- /app/bin/flurmctld remote_console
> flurm_cluster:status().
```

## Architecture

```
                    ┌─────────────────────────────────┐
                    │         Kubernetes Cluster       │
                    └─────────────────────────────────┘
                                    │
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
         ▼                          ▼                          ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   flurmctld-0   │      │   flurmctld-1   │      │   flurmctld-2   │
│  (StatefulSet)  │◄────►│  (StatefulSet)  │◄────►│  (StatefulSet)  │
└────────┬────────┘      └────────┬────────┘      └────────┬────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                      ┌───────────▼───────────┐
                      │  flurmctld Service    │
                      │    (ClusterIP)        │
                      └───────────────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│    flurmnd      │      │    flurmnd      │      │    flurmnd      │
│   (DaemonSet)   │      │   (DaemonSet)   │      │   (DaemonSet)   │
│    node-1       │      │    node-2       │      │    node-N       │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

## Configuration

### Controller Replicas

For production, use at least 3 controllers for Raft quorum:

```yaml
spec:
  replicas: 3  # Minimum for HA
```

### Resource Limits

Adjust based on cluster size:

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "1"
  limits:
    memory: "4Gi"
    cpu: "4"
```

### Storage

Controllers use PersistentVolumeClaims for Raft state:

```yaml
volumeClaimTemplates:
- metadata:
    name: data
  spec:
    storageClassName: fast-ssd  # Use fast storage for Raft
    resources:
      requests:
        storage: 10Gi
```

## Scaling

### Add Compute Nodes

```bash
# Label new nodes
kubectl label node new-node flurm.io/compute-node=true

# DaemonSet will automatically deploy flurmnd
```

### Scale Controllers

```bash
# Scale up (for increased capacity)
kubectl scale statefulset flurmctld -n flurm --replicas=5

# Scale down (maintain odd numbers for Raft)
kubectl scale statefulset flurmctld -n flurm --replicas=3
```

## Monitoring

### Prometheus Metrics

Controllers expose metrics on port 9090:

```bash
# Port-forward to access metrics
kubectl port-forward -n flurm svc/flurmctld 9090:9090

# Access metrics
curl http://localhost:9090/metrics
```

### Grafana Dashboard

Import the FLURM dashboard from `deploy/grafana/flurm-dashboard.json`.

## Troubleshooting

### Controllers Not Forming Cluster

1. Check network policy allows Erlang distribution (port 4369)
2. Verify cookie matches on all pods
3. Check headless service DNS resolution

```bash
kubectl exec -n flurm flurmctld-0 -- nslookup flurmctld-headless
```

### Node Daemon Not Registering

1. Verify controller service is accessible
2. Check node daemon logs:

```bash
kubectl logs -n flurm -l app.kubernetes.io/component=node-daemon
```

### Raft State Corruption

1. Stop all controllers
2. Delete PVC data
3. Restart controllers

```bash
kubectl delete pvc -n flurm -l app.kubernetes.io/component=controller
kubectl rollout restart statefulset -n flurm flurmctld
```
