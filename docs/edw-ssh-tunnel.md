# EDW SSH Tunnel Reliability

Last reviewed: 2026-09-22

## Issue

Dagster EDW assets intermittently failed while opening a pymssql connection:

```text
dagster._core.execution.plan.utils.RetryRequestedFromPolicy
pymssql.exceptions.OperationalError: (20002, ...
TDS server connection failed (127.0.0.1))
```

`RetryRequestedFromPolicy` is Dagster applying the asset retry policy. The
underlying failure is DB-Lib error `20002`: nothing accepted the SQL Server
connection through the local forward at `127.0.0.1:1433`.

EDW is reached through an SSH forwarder in the Dagster daemon pod. The shared
Vault configuration for `tnl2.s.uw.edu` historically used plain `ssh` with
`ServerAliveInterval=300`. After the proxy moved to the cloud, it could
terminate long-running connections. Plain `ssh` exited when that happened and
did not establish a new tunnel, leaving later EDW connections unavailable.

## Fix

The test and production Dagster daemon forwarders now:

- use `uw-ssh-client:c73534d`, which includes `autossh`
- run `autossh -M 0` with `ServerAliveInterval=60` and
  `ServerAliveCountMax=3`
- use `ExitOnForwardFailure=yes` so an unusable forward fails visibly
- set native sidecar `restartPolicy: Always`

This follows the reconnect pattern used by `canvas-training-provisioner`. The
deployment continues to read the proxy hostname, username, and private key
from `shared/proxies/uw_ssh_tunnel`, but intentionally does not execute that
Vault entry's older plain-SSH command.

`dawgpath_data_pipeline.dao.edw._run_query()` also retries DB-Lib error `20002`
up to three connection attempts with 2- and 4-second delays. Other pymssql
errors are raised immediately. This retry bridges a short reconnect window; it
does not replace `autossh` recovery.

## Verification

Find the current daemon pod after each rollout because its generated name
changes:

```bash
NS=dawgpath-data-pipeline-test
POD=$(kubectl get pods -n "$NS" \
  -l app.kubernetes.io/name=dagster-daemon \
  -o jsonpath='{.items[0].metadata.name}')

kubectl get pod -n "$NS" "$POD" \
  -o jsonpath='{.status.initContainerStatuses[?(@.name=="forwarder")].ready}{" restarts="}{.status.initContainerStatuses[?(@.name=="forwarder")].restartCount}{"\n"}'
kubectl logs -n "$NS" "$POD" -c forwarder --since=60m
```

The chart renders `forwarder` as a restartable init container, so its status is
under `initContainerStatuses`, not `containerStatuses`.

Then materialize a small EDW asset such as `fetch_curric_data` or
`fetch_sr_major_data`. Repeat after an idle period long enough to exercise the
proxy timeout. Success means:

- the asset completes without DB-Lib error `20002`
- `forwarder` remains ready, or reconnects and becomes ready again
- no repeated SSH authentication or forwarding errors appear in its logs

On 2026-09-22, the test daemon remained idle for approximately 30 minutes and
then completed an EDW asset successfully. The forwarder was ready on image
`c73534d`, had zero restarts, and logged only initial host-key registration.

## Diagnosis

If the error returns, capture these before restarting the pod:

```bash
kubectl describe pod -n "$NS" "$POD"
kubectl logs -n "$NS" "$POD" -c forwarder --since=2h
kubectl logs -n "$NS" "$POD" -c forwarder --previous
kubectl get pod -n "$NS" "$POD" -o yaml
```

Interpret the results as follows:

- Connection refused on `127.0.0.1` means the local forward is not listening.
- SSH authentication or host-resolution errors point to proxy identity,
  secret, DNS, or network configuration.
- A healthy local forward followed by SQL login errors points to EDW rather
  than the SSH tunnel.
- Frequent sidecar restarts indicate `autossh` is recovering repeatedly and
  the proxy or network path still needs investigation.
