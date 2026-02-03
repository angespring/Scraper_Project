#!/usr/bin/env bash
set -euo pipefail

# new_build.sh
# Tight new build bootstrapper:
# - resolves pre-branch changes safely (tracked vs untracked)
# - creates a branch with a consistent name and auto-suffix if needed
# - optionally pushes and sets upstream
#
# Usage:
#   ./new_build.sh
#   ./new_build.sh --no-push
#   ./new_build.sh --dry-run
#   BASE_BRANCH=main ./new_build.sh

BASE_BRANCH="${BASE_BRANCH:-main}"
DO_PUSH=1
DRY_RUN=0

for arg in "$@"; do
  case "$arg" in
    --no-push) DO_PUSH=0 ;;
    --dry-run) DRY_RUN=1 ;;
    *)
      echo "Unknown arg: $arg"
      echo "Valid args: --no-push --dry-run"
      exit 1
      ;;
  esac
done

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    eval "$@"
  fi
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

lower() {
  # macOS bash 3 safe lowercase
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

confirm() {
  local prompt="$1"
  local reply
  read -r -p "$prompt [y/N]: " reply
  reply="$(lower "$reply")"
  [[ "$reply" == "y" || "$reply" == "yes" ]]
}

choose() {
  # choose "Prompt" "1) ..." "2) ..."
  local prompt="$1"
  shift
  echo
  echo "$prompt"
  local opt
  for opt in "$@"; do
    echo "  $opt"
  done
  echo
  local selection
  read -r -p "Select an option (number): " selection
  printf '%s' "$selection"
}

require_git_repo() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "Not inside a git repository."
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

repo_root() {
  git rev-parse --show-toplevel
}

slugify() {
  # Lowercase, spaces to dashes, keep alnum and dashes, collapse repeats, trim ends
  echo "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[[:space:]]+/-/g' \
    | sed -E 's/[^a-z0-9-]+/-/g' \
    | sed -E 's/-+/-/g' \
    | sed -E 's/^-+//; s/-+$//'
}

append_gitignore() {
  local pattern="$1"
  if [[ -f .gitignore ]] && grep -Fxq "$pattern" .gitignore; then
    return 0
  fi
  echo "$pattern" >> .gitignore
}

list_tracked_changes() {
  git status --porcelain | awk 'substr($0,1,2)!="??"{print $0}'
}

list_untracked() {
  git status --porcelain | awk 'substr($0,1,2)=="??"{print substr($0,4)}'
}

count_tracked() {
  git status --porcelain | awk 'substr($0,1,2)!="??"{c++} END{print c+0}'
}

count_untracked() {
  git status --porcelain | awk 'substr($0,1,2)=="??"{c++} END{print c+0}'
}

has_any_changes() {
  [[ -n "$(git status --porcelain)" ]]
}

# Safety: treat these as "artifact-like" paths. Committing them should be explicit.
is_suspicious_untracked() {
  # Reads paths on stdin, returns 0 if any suspicious path is present
  # patterns cover common debug output in your repo
  awk '
    BEGIN { suspicious=0 }
    {
      p=$0
      if (p ~ /^debug_.*\//) suspicious=1
      if (p ~ /^debug_.*$/) suspicious=1
      if (p ~ /_debug\.html$/) suspicious=1
      if (p ~ /^yc_debug\.html$/) suspicious=1
      if (p ~ /^Code Archive\//) suspicious=1
      if (p ~ /^\.DS_Store$/) suspicious=1
      if (p ~ /^__pycache__\//) suspicious=1
    }
    END { exit (suspicious ? 0 : 1) }
  '
}

prebranch_gate() {
  if ! has_any_changes; then
    echo "Working tree is clean."
    return 0
  fi

  echo "Found changes BEFORE branch creation:"
  git status --porcelain

  local tracked_count untracked_count
  tracked_count="$(count_tracked)"
  untracked_count="$(count_untracked)"

  # Fastest safe escape hatch
  if confirm "Do you want to stash everything and continue (recommended if unsure)?"; then
    local smsg
    read -r -p "Stash message (default: pre-build stash): " smsg
    smsg="${smsg:-pre-build stash}"
    run "git stash push -u -m \"${smsg//\"/\\\"}\""
    echo "Stashed tracked and untracked changes."
    return 0
  fi

  # Handle tracked changes
  if [[ "$tracked_count" -gt 0 ]]; then
    echo
    echo "Tracked changes:"
    list_tracked_changes

    local sel
    sel="$(choose "Tracked changes detected. What do you want to do?" \
      "1) Commit tracked changes (snapshot) on current branch" \
      "2) Discard tracked changes (restore to HEAD)" \
      "3) Abort")"

    case "$sel" in
      1)
        local msg
        read -r -p "Commit message (default: chore: pre-build snapshot): " msg
        msg="${msg:-chore: pre-build snapshot}"
        run "git add -A"
        run "git commit -m \"${msg//\"/\\\"}\""
        echo "Committed tracked changes."
        ;;
      2)
        if confirm "This will discard tracked changes. Continue?"; then
          run "git restore --staged ."
          run "git restore ."
          echo "Discarded tracked changes."
        else
          die "Aborted."
        fi
        ;;
      3|*)
        die "Aborted."
        ;;
    esac
  fi

  # Handle untracked after tracked resolution
  untracked_count="$(count_untracked)"
  if [[ "$untracked_count" -gt 0 ]]; then
    echo
    echo "Untracked items:"
    list_untracked

    local sel2
    sel2="$(choose "Untracked items detected. What do you want to do?" \
      "1) Ignore untracked items (adds rules to .gitignore, keeps files)" \
      "2) Delete untracked items (git clean)" \
      "3) Commit untracked items (snapshot) on current branch" \
      "4) Abort")"

    case "$sel2" in
      1)
        echo
        echo "Paste one ignore pattern per line. Blank line to finish."
        echo "Examples:"
        echo "  debug_*/"
        echo "  yc_debug.html"
        echo "  *.log"
        echo
        while true; do
          local pat
          read -r -p "Ignore pattern: " pat
          [[ -z "$pat" ]] && break
          append_gitignore "$pat"
        done

        # Also offer safe defaults that match your typical artifacts
        if confirm "Add safe default ignores too (recommended)?"; then
          append_gitignore ".DS_Store"
          append_gitignore "__pycache__/"
          append_gitignore "*.pyc"
          append_gitignore ".pytest_cache/"
          append_gitignore "*.log"
          append_gitignore "yc_debug.html"
          append_gitignore "*_debug.html"
          append_gitignore "debug_*/"
        fi

        echo "Updated .gitignore."
        if confirm "Commit the .gitignore update now?"; then
          run "git add .gitignore"
          run "git commit -m \"chore: update local ignores\""
          echo "Committed .gitignore update."
        else
          echo "Left .gitignore uncommitted."
        fi
        ;;
      2)
        echo
        echo "Preview delete:"
        git clean -nd
        if confirm "Proceed with delete (git clean -fd)?"; then
          run "git clean -fd"
          echo "Deleted untracked items."
        else
          die "Aborted."
        fi
        ;;
      3)
        # Safety lock: require explicit confirmation if artifacts are present
        if list_untracked | is_suspicious_untracked; then
          echo
          echo "Safety check: suspicious untracked artifacts detected (debug output or local files)."
          echo "These are usually better ignored or deleted."
          if ! confirm "Commit anyway?"; then
            die "Aborted commit. Choose ignore or delete instead."
          fi
        fi

        local msg2
        read -r -p "Commit message (default: chore: commit untracked items): " msg2
        msg2="${msg2:-chore: commit untracked items}"
        run "git add -A"
        run "git commit -m \"${msg2//\"/\\\"}\""
        echo "Committed untracked items."
        ;;
      4|*)
        die "Aborted."
        ;;
    esac
  fi

  # Final clean check
  if has_any_changes; then
    echo
    echo "There are still changes present:"
    git status --porcelain
    die "Resolve remaining changes, then re-run."
  fi

  echo "Working tree is clean after pre-branch handling."
}

update_base_branch_optional() {
  echo
  echo "Base branch: ${BASE_BRANCH}"
  if confirm "Switch to base branch and pull latest before branching?"; then
    run "git checkout \"${BASE_BRANCH}\""
    if confirm "Pull latest from origin/${BASE_BRANCH} with ff-only?"; then
      run "git pull --ff-only"
    fi
  else
    echo "Skipping base branch update."
  fi
}

next_available_branch_name() {
  # If branch exists, add -2, -3, ...
  local base="$1"
  local candidate="$base"
  local i=2
  while git show-ref --verify --quiet "refs/heads/${candidate}"; do
    candidate="${base}-${i}"
    i=$((i+1))
  done
  printf '%s' "$candidate"
}

create_build_branch() {
  local build_name slug date base_name branch_name

  echo
  read -r -p "Enter build name (example: fix/location-gates): " build_name
  [[ -n "${build_name// }" ]] || die "Build name cannot be empty."

  # Option B: Use build name exactly as typed
  base_name="$build_name"
  branch_name="$(next_available_branch_name "$base_name")"


  echo "New branch will be: ${branch_name}"

  # Disallow spaces in branch names (git allows it, but it is painful)
  if printf '%s' "$build_name" | grep -q '[[:space:]]'; then
    die "Build name cannot contain spaces. Use / or _ instead."
  fi


  local cb
  cb="$(current_branch)"
  if [[ "$cb" != "$BASE_BRANCH" ]]; then
    if confirm "Currently on '${cb}'. Switch to '${BASE_BRANCH}' before creating the build branch?"; then
      run "git checkout \"${BASE_BRANCH}\""
    else
      echo "Using current branch as branch point: ${cb}"
    fi
  fi

  run "git checkout -b \"${branch_name}\""
  echo "Created and checked out: ${branch_name}"

  if [[ "$DO_PUSH" -eq 1 ]]; then
    if confirm "Push branch to origin and set upstream?"; then
      run "git push -u origin \"${branch_name}\""
      echo "Pushed and set upstream."
    else
      echo "Skipped push. Push later with:"
      echo "  git push -u origin ${branch_name}"
    fi
  else
    echo "Push skipped due to --no-push."
  fi
}

main() {
  require_git_repo

  echo "Repo: $(repo_root)"
  echo "Current branch: $(current_branch)"

  # Resolve pre-branch changes first so branch starts clean and predictable
  prebranch_gate

  # Optional base update
  update_base_branch_optional

  # Ensure still clean after any checkout or pull
  prebranch_gate

  # Create branch
  create_build_branch

  echo
  echo "All set."
}

main
