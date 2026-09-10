# WebSocket Support for the Dagster UI

Status: proposed, not implemented. Deployment currently runs the HTTP-only
option (Django reverse proxy), which works for everything except live log
streaming.

Three options are recorded below. Plan B is the smallest change. Plan C is
likely the better long-term answer and has a working precedent in the org.

## Problem

The Dagster UI streams run logs over a WebSocket (GraphQL subscriptions at
`/dagster/graphql`). The current path to the UI is:

```
kgateway -> Service :80 -> nginx :8000 -> gunicorn (WSGI) -> DagsterProxyView
         -> 127.0.0.1:3000 (dagster-webserver sidecar)
```

`DagsterProxyView` in `dawgpath_pipeline_admin/views/dagster.py` proxies with
`requests`, and gunicorn/WSGI cannot perform an HTTP upgrade. So the UI loads
and runs can be launched, but run pages do not stream; they need a reload.

kgateway is **not** the blocker. Envoy passes `Upgrade: websocket` through on
HTTP routes.

The chart is also not able to route around it: `templates/gateway.yaml` in
`django-production-chart` emits exactly one rule per hostname, with the path,
backend, and port hardcoded, and no values hook for additional rules.

```yaml
rules:
  - matches:
      - path:
          type: PathPrefix
          value: /
    backendRefs:
      - name: {{ $.Values.releaseIdentifier }}
        port: 80
```

## Approach (Plan B)

`django-container` already runs nginx in front of gunicorn (`conf/nginx.conf`,
upstream `app_server` over a unix socket). nginx handles WebSocket upgrades
natively, so move the `/dagster` proxy from Django to nginx and keep Django as
the authorization decision point via `auth_request`.

```
kgateway -> nginx :8000 --(auth_request)--> gunicorn -> /dagster/auth-check
                       \--(proxy_pass + upgrade)--> 127.0.0.1:3000
```

The Dagster sidecar stays bound to loopback with `--path-prefix /dagster`, so
it remains unreachable except through nginx, and SAML remains the only way in.

## Changes in `uw-it-aca/django-container`

Keep the base image generic. Do not add DawgPath-specific locations.

1. Add an include for an optional app-supplied snippet in `conf/nginx.conf`,
   inside the `server` block, e.g.:

   ```nginx
   include /app/conf/nginx-extra.conf*;
   ```

   The trailing `*` makes nginx skip it when absent, so existing apps are
   unaffected.

2. Confirm the snippet path survives image build and that nginx starts when the
   file is missing.

Unverified before implementing: whether `conf/nginx.conf` already has an
include mechanism, and how static/media locations are ordered relative to a new
prefix location.

## Changes in this repository

1. Add `dawgpath_pipeline_admin/conf/nginx-extra.conf` (copied to
   `/app/conf/nginx-extra.conf` by the Dockerfile):

   ```nginx
   location /dagster {
       auth_request /_dagster_auth;
       error_page 401 = @dagster_login;

       proxy_pass http://127.0.0.1:3000;
       proxy_http_version 1.1;
       proxy_set_header Upgrade $http_upgrade;
       proxy_set_header Connection "upgrade";
       proxy_set_header Host $host;
       proxy_buffering off;
       proxy_read_timeout 3600s;
   }

   location = /_dagster_auth {
       internal;
       proxy_pass http://app_server/dagster/auth-check;
       proxy_pass_request_body off;
       proxy_set_header Content-Length "";
   }

   location @dagster_login {
       return 302 /saml/login?next=$request_uri;
   }
   ```

   Notes:
   - `auth_request` forwards the original `Cookie` header, so the Django
     session is available to the check.
   - `error_page 401` matters: without it a browser gets a bare 401 instead of
     a SAML redirect.
   - `proxy_buffering off` and a long `proxy_read_timeout` are needed for log
     streaming; the gateway `requestTimeouts` (90s) may also need raising for
     the `/dagster` route.

2. Replace `DagsterProxyView` with a lightweight authorization endpoint that
   returns 200/401/403 and no body. Reuse the existing `has_dagster_access()`
   helper and `DAGSTER_ACCESS_GROUP` setting; both already exist.

3. Route `^dagster/auth-check$` to that view. Remove the catch-all
   `^dagster(/.*)?$` proxy route, since nginx handles the path once this lands.

4. Drop the direct `requests` dependency from `setup.py` if nothing else uses
   it.

5. Tests: keep the auth boundary coverage in
   `dawgpath_pipeline_admin/tests/test_dagster_proxy.py`, retargeted at
   `auth-check` (anonymous -> 401, wrong group -> 403, member -> 200). The
   proxy-behavior tests can be deleted with the view.

## Deployment values

No change needed. `sidecarContainers.dagster-webserver` and the loopback bind
already match this design. If the gateway timeout proves too short for
long-lived subscriptions, raise `gateway.requestTimeouts` in
`docker/test-values.yml` and `docker/prod-values.yml`.

## Verification

1. Local: run the gateway container and Dagster sidecar side by side, confirm
   `/dagster` loads and the run page streams logs without reload.
2. Confirm an unauthenticated request to `/dagster` redirects to SAML login,
   not a bare 401.
3. Confirm a user outside `DAGSTER_ACCESS_GROUP` gets 403.
4. Confirm `curl -I` on `/dagster/graphql` without a session does not reach
   Dagster.
5. Deploy to test and watch for nginx `auth_request` latency; it adds one
   subrequest per request, including static asset fetches under `/dagster`.

## Rollback

Revert the repo changes and restore `DagsterProxyView`. The base-image include
is inert on its own, so it can stay.

## Alternative: gateway-level UW OIDC (Plan C)

`gcp-flux-rttl-eval` already authenticates a non-Django workload (JupyterHub,
`quay.io/jupyterhub/k8s-hub`) with UW SSO at the kgateway layer. Recorded in
`copilot-knowledge/inventory/deployments/repos/gcp-flux-rttl-eval.yaml`:

| Resource | File | Line |
|---|---|---|
| `Backend` | `deploy/uw-oidc.yaml` | 12 |
| `GatewayExtension` | `deploy/uw-oidc.yaml` | 24 |
| `BackendConfigPolicy` | `deploy/uw-oidc.yaml` | 71 |
| `ExternalSecret` | `deploy/uw-oidc-secret.yaml` | 12 |
| `HTTPRoute` | `deploy/oauth2-callback.yaml` | 17 |

That is the Gateway API ext-auth shape: a `Backend` for the UW OIDC provider, a
`GatewayExtension` wiring it in as the auth extension, an OIDC client secret
from Vault, and a dedicated route for the OAuth2 callback.

Why it fits here: authentication happens at the edge, so Envoy proxies the
WebSocket upgrade straight to the Dagster sidecar. No Django in the request
path, no nginx snippet, and `DagsterProxyView` is deleted rather than replaced.
It also uses UW OIDC instead of SAML, so the gateway owns the login flow.

Still required:

- A route to the Dagster port. The shared chart still emits only one rule per
  hostname, so this needs the same chart change as Plan A, or DawgPath-specific
  manifests in the Flux repo.
- Authorization. OIDC at the edge proves identity; group membership
  (`DAGSTER_ACCESS_GROUP` today) must move into the gateway policy or stay as a
  check inside Dagster's reachable surface.
- An OIDC client registration and secret in Vault.

Unverified: the file contents could not be read (the repo is private, so the
inventory records resource kinds and line numbers only, not field values). The
RTTL eval cluster may also wire `default-gateway`/`kgateway-system` differently
from `gcp-flux-dev`/`gcp-flux-prod`, and `GatewayExtension` availability in
those clusters needs confirming. The RTTL owners are the people to ask.

## Alternative: edge ext-auth against Django (Plan A)

Plan A moved authentication to the edge while keeping Django as the decision
point: extend `django-production-chart` with values-driven extra HTTPRoute
rules plus a kgateway `TrafficPolicy` using ext-auth, and expose port 3000 on
the Service. That is architecturally cleaner than Plan B and would delete the
proxy entirely, but it needs ext-auth infrastructure, platform-owner wiring,
and confirmation of kgateway's ext-auth CRD shape against the deployed version.
Plan B was preferred as the smaller change that keeps a single SAML boundary.

Note that `TrafficPolicy` is already in use across `gcp-flux-dev` and
`gcp-flux-prod` for the chart's rate-limit and buffer features, so the CRD is
live in the main clusters even though ext-auth is not yet used there.
