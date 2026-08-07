#!/bin/bash
# DEPRECATED shim -- retained so existing callers keep working.
# The single entrypoint is scripts/daily_mbb_data_processor.sh (design D21);
# python is its default language, so this just forwards with -l python.
echo "::warning ::daily_mbb_python_processor.sh is deprecated; use scripts/daily_mbb_data_processor.sh -l python" >&2
exec bash "$(dirname "$0")/daily_mbb_data_processor.sh" "$@" -l python
