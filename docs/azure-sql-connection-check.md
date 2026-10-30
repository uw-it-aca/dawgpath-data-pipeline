# Azure SQL Connection Check

Run the same connection-only diagnostic locally and in the test Dagster daemon. It reports the configured host/port, resolved IPs, TCP reachability, and whether Microsoft ODBC Driver 18 can complete a login. It does not print credentials or run an application query.

## Local

Use an environment with the project dependencies, the Python `pyodbc` package, and Microsoft's ODBC Driver 18 plus unixODBC installed. This checkout's local virtual environment currently lacks `pyodbc`, and the host does not have `odbcinst`, so it is not ready for a live local attempt yet. Set the non-secret connection settings and enter the password at the prompt so it is not stored in shell history:

```sh
export AZSQL_SERVER=studentanalytics-prod.database.windows.net
export AZSQL_PORT=1433
export AZSQL_DATABASE=StudentAnalytics
export AZSQL_AUTHENTICATION=ActiveDirectoryPassword
export AZSQL_DRIVER='ODBC Driver 18 for SQL Server'
export AZSQL_USER='your-authorized-account'
read -r -s -p 'Azure SQL password: ' AZSQL_PASSWORD
printf '\n'
export AZSQL_PASSWORD
./venv/bin/python -m dawgpath_data_pipeline.utilities.check_azure_sql_connection
unset AZSQL_PASSWORD
```

Run this from the same local network/VPN context in which the application is known to connect. Record the full output, especially resolved addresses and SQLSTATE, but do not share credentials.

## Test Pod

Run from the Dagster daemon pod, because that is where test runs and the SSH forwarder are configured. Find its pod and application-container name:

```sh
kubectl get pods -n dawgpath-data-pipeline-test -o wide | rg dagster-daemon
kubectl get pod -n dawgpath-data-pipeline-test POD_NAME \
  -o jsonpath='{.spec.containers[*].name}'
```

Copy the standalone probe into the running pod, run it in the application container, then remove it. This does not require a new image deployment. Azure credentials are injected into the pod as environment variables:

```sh
NAMESPACE=dawgpath-data-pipeline-test
POD_NAME=dagster-daemon-REPLACE_WITH_RUNNING_POD
APPLICATION_CONTAINER=dawgpath-data-pipeline
PROBE=dawgpath_data_pipeline/utilities/check_azure_sql_connection.py

kubectl cp "$PROBE" "$NAMESPACE/$POD_NAME:/tmp/check_azure_sql_connection.py" \
  -c "$APPLICATION_CONTAINER"
kubectl exec -n "$NAMESPACE" "$POD_NAME" -c "$APPLICATION_CONTAINER" -- \
  env LD_PRELOAD=libnss_wrapper.so \
      NSS_WRAPPER_HOSTS=/app/project/azure-sql-hosts \
      PYTHONPATH=/app \
      /app/bin/python /tmp/check_azure_sql_connection.py
probe_status=$?
kubectl exec -n "$NAMESPACE" "$POD_NAME" -c "$APPLICATION_CONTAINER" -- \
  rm -f /tmp/check_azure_sql_connection.py
test "$probe_status" -eq 0
```

`kubectl exec` starts a separate process and may not inherit resolver variables that were added only to the Dagster daemon command. The invocation above explicitly matches the daemon's `LD_PRELOAD` and `NSS_WRAPPER_HOSTS` settings; verify those paths against PID 1 before running if deployment values change. The diagnostic reports the effective runtime host and port. A nonzero exit is expected when the connection fails; the cleanup command still runs before the final status check.

## Interpreting Results

- DNS resolving to a loopback address and TCP succeeding indicates the process reaches a local forwarder. If ODBC then returns `40615` with the cluster egress IP, that is consistent with Azure Redirect sending the backend connection outside the SSH forward; include the SQLSTATE and client IP in the report.
- DNS resolving to a public Azure address means that process is not using a loopback hostname override.
- TCP failure to loopback usually means the local forward is absent or not listening; a SQL login has not yet been attempted.
- ODBC success confirms a complete authenticated connection. The probe closes it without issuing a query.

Compare both outputs, including timestamp, environment, resolved IP, TCP result, and SQLSTATE. Redact usernames or public IPs if required by local policy; never include passwords or a full ODBC connection string.