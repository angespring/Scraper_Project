#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"

# Patterns that typically create noisy output and ruin single-line progress.
# Includes:
#  - log_line("DEBUG"...), log_line("BIV DEBUG"...)
#  - log_print("[BIV DEBUG"...)
#  - plain print(...) and builtins.print(...)
#  - 🐞 marker and "BIV DEBUG" label
PATTERN='(^|[^#])\b(log_line|log_print)\s*\(\s*["'\''](DEBUG|BIV DEBUG|TRACE|BIV)\b|(^|[^#])\b(print|builtins\.print|ts_pprint)\s*\(|🐞|\bBIV DEBUG\b'

echo "Scanning for debug output in: $ROOT"
echo

# Build a null-delimited file list (handles spaces safely)
if command -v rg >/dev/null 2>&1; then
  find "$ROOT" -type f \( -name "*.py" -o -name "*.sh" \) \
    -not -path "*/.venv/*" \
    -not -path "*/venv/*" \
    -not -path "*/__pycache__/*" \
    -not -path "*/Code Archive/*" \
    -not -path "*/.git/*" \
    -print0 \
  | xargs -0 rg --pcre2 -n --no-heading --color=never "$PATTERN" \
  | sed 's/\r$//'
else
  echo "ripgrep (rg) not found; using grep fallback."
  echo
  find "$ROOT" -type f \( -name "*.py" -o -name "*.sh" \) \
    -not -path "*/.venv/*" \
    -not -path "*/venv/*" \
    -not -path "*/__pycache__/*" \
    -not -path "*/Code Archive/*" \
    -not -path "*/.git/*" \
    -print0 \
  | while IFS= read -r -d '' f; do
      grep -nH -E '^[[:space:]]*[^#].*(log_line\("DEBUG"|log_line\("BIV DEBUG"|log_print\("\[BIV DEBUG|BIV DEBUG|🐞|builtins\.print\(|[^a-zA-Z]print\()' "$f" || true
    done
fi

echo
echo "To narrow to Built In debug only:"
echo "  ./find_debug_prints.sh . | grep -E 'BIV DEBUG|builtin|🐞'"
