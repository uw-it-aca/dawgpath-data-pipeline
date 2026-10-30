# SWS TLS Configuration

The pipeline uses the standard `Live` REST-client backend and the mounted UWCA
client certificate/key. No custom Python transport is required.

Both deployment values files follow the existing
[Eventcal](https://github.com/uw-it-aca/eventcal/blob/main/docker/prod-values.yml)
and [Panopto Scheduler](https://github.com/uw-it-aca/django-panopto-scheduler/blob/main/docker/prod-values.yml)
compatibility pattern:

- Mount the `openssl-conf` ConfigMap read-only at `/ssl` on all runtime containers,
  including the Dagster daemon and webserver.
- Set `OPENSSL_CONF=/ssl/openssl.cnf` before Python starts. The configuration
  requires TLS 1.2 or newer and permits legacy UWCA chains with
  `CipherString = DEFAULT:@SECLEVEL=1`.
- Set `RESTCLIENTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt`, the Ubuntu
  system trust bundle. The standalone pipeline settings supply the same default;
  environment configuration can override it.
- Keep `RESTCLIENTS_SWS_VERIFY_HTTPS=True` and the existing per-environment
  client certificate/key settings. Do not disable server or hostname verification.

This is a process-wide OpenSSL compatibility policy, not an SWS-only exception.
It follows other UW applications but lowers the normal OpenSSL security level.
Replace the legacy certificate chain when feasible, then remove the compatibility
policy after testing with normal security. Python/urllib3 versions may explicitly
set their own cipher policy, so a deployed Python lookup is required to confirm
this configuration actually works; a successful curl request alone is insufficient.

## Diagnosis

On October 29, investigation of the October 16 SWS refresh found two issues:

1. The Python traceback failed in `SSLContext.load_verify_locations()` with
   `FileNotFoundError`. The REST client's default CA path was incompatible with
   the container filesystem.
2. Curl with the mounted client certificate/key failed with `ca key too small`.
   A one-off `--ciphers 'DEFAULT@SECLEVEL=1'` request returned HTTP 200 with TLS
   verification result 0. This established current SWS access with compatibility
   enabled; it did not identify which certificate in the chain had the weak key.

The refresh logged individual errors but still returned SUCCESS with zero rows.
This deployment change does not fix that separate error-reporting behavior.

## Validate After Deployment

Deploy the new image/settings and restart runtime processes so the OpenSSL policy
is loaded at interpreter startup. Editing files or exporting `OPENSSL_CONF` in a
shell does not reconfigure already-running daemon processes. Do not run the full
refresh until a single Python request succeeds.

Inside the restarted daemon container, confirm paths without displaying key data:

```bash
printf 'OPENSSL_CONF=%s\n' "$OPENSSL_CONF"
test -r "$OPENSSL_CONF"
test -r "$RESTCLIENTS_CA_BUNDLE"
test -r "$RESTCLIENTS_SWS_CERT_FILE"
test -r "$RESTCLIENTS_SWS_KEY_FILE"
```

Then run a read-only request using the actual Python wrapper, not curl:

```bash
/app/bin/python - <<'PY'
from dawgpath_data_pipeline.dao.sws import get_course

course = get_course(2026, 4, "CSE", 142)
if course is None:
    raise SystemExit("No course returned; choose a known valid course/term.")
description = getattr(course, "course_description", None)
print("SWS lookup: OK")
print("Description populated:", bool(description and description.strip()))
print("Description characters:", len(description or ""))
PY
```

A lookup failure should be diagnosed before requesting thousands of courses. If
Python still reports `ca key too small`, inspect the deployed REST-client/urllib3
versions and effective SSL context rather than assuming `OPENSSL_CONF` controls
an explicitly overridden Python cipher policy. Do not disable verification.

After the request succeeds, run `refresh_sws_course_descriptions`, check populated
description coverage in the pipeline database, and re-export into a new immutable
release. Do not edit the existing published release to replace null descriptions.

Configuration regression check from the checkout:

```bash
python -m unittest discover -s dawgpath_data_pipeline/tests -p test_instance_config.py
```