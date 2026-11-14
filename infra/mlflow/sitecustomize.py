"""Opt-in TLS verification bypass for self-signed upstream endpoints.

The MLflow AI Gateway uses aiohttp's default TLS context, which has no
"insecure" switch. Setting LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY=true replaces
the default context with one that does not verify the upstream certificate.
Keep the default for public providers.
"""

from __future__ import annotations

import os
import ssl

if os.environ.get("LLM_UPSTREAM_INSECURE_SKIP_TLS_VERIFY", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}:
    _create_default_context = ssl.create_default_context

    def _unverified_default_context(*args, **kwargs):
        context = _create_default_context(*args, **kwargs)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    ssl.create_default_context = _unverified_default_context
