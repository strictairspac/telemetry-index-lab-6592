#!/usr/bin/env bash
# Stdlib-only verification: no third-party packages or MySQL server required.
# Run from anywhere; uses isolated mode (-I) so planted modules cannot load.
set -euo pipefail
cd "$(dirname "$0")"
python3 -I test_lock_watch.py
python3 -I test_pipeline_integration.py
