#!/bin/bash
set -e
# Install fairway from mounted source (editable, no-deps since deps are baked in)
pip install -e /app[dev] --quiet --no-deps
exec "$@"
