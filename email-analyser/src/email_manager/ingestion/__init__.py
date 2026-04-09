"""Ingestion package — email and calendar sync clients."""

import os

# httplib2 (used by the Google API client) ignores standard SSL env vars and
# uses certifi's bundled CA bundle, which doesn't include custom/MITM proxy CAs.
# Point it at the system CA bundle when SSL_CERT_FILE is set.
_ca_bundle = os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE")
if _ca_bundle and os.path.exists(_ca_bundle):
    import httplib2  # noqa: E402
    httplib2.CA_CERTS = _ca_bundle
