#!/usr/bin/env bash
# Simple helper to save work to GitHub

# Use first argument as message, or a default with timestamp
MSG="$1"
if [ -z "$MSG" ]; then
  MSG="Auto save on $(date +"%Y-%m-%d %H:%M:%S")"
fi

echo ">> Staging changes..."
git add .

echo ">> Committing with message:"
echo "   $MSG"
git commit -m "$MSG"

echo ">> Pushing to remote..."
git push

echo ">> Done."
