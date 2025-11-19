#!/usr/bin/env python3
"""Generate Markdown release notes from git history.

This is a lightweight helper that Product/PM stakeholders can run to craft
polished release notes without manually copying commit metadata.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import subprocess
import sys
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "RELEASE_NOTES.md"


def run_git(args: list[str]) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def collect_commits(
    from_ref: str | None,
    to_ref: str,
    since: str | None,
    include_merges: bool,
) -> list[dict[str, str]]:
    """Return git log rows between the selected refs."""
    pretty = "%H%x09%an%x09%ad%x09%s"
    cmd = ["log", "--date=short", f"--pretty=format:{pretty}"]
    if since:
        cmd.append(f"--since={since}")
    if not include_merges:
        cmd.append("--no-merges")
    revision = f"{from_ref}..{to_ref}" if from_ref else to_ref
    cmd.append(revision)

    raw = run_git(cmd)
    commits: list[dict[str, str]] = []
    for line in raw.splitlines():
        try:
            sha, author, date, subject = line.split("\t", 3)
        except ValueError:
            continue
        commits.append(
            {
                "hash": sha,
                "short": sha[:7],
                "author": author,
                "date": date,
                "subject": subject.strip(),
            }
        )
    return commits


def build_highlights(commits: Iterable[dict[str, str]]) -> str:
    lines = [
        f"- {c['subject']} (`{c['short']}` by {c['author']} on {c['date']})"
        for c in commits
    ]
    return "\n".join(lines) if lines else "- _No commits were found for this range._"


def build_commit_table(commits: Iterable[dict[str, str]]) -> str:
    rows = [
        "| Commit | Description | Author | Date |",
        "| --- | --- | --- | --- |",
    ]
    for c in commits:
        rows.append(
            f"| `{c['short']}` | {c['subject']} | {c['author']} | {c['date']} |"
        )
    if len(rows) == 2:
        rows.append("| — | _No commits in range._ | — | — |")
    return "\n".join(rows)


def diff_stats(from_ref: str | None, to_ref: str) -> str:
    if not from_ref:
        return ""
    diff_range = f"{from_ref}..{to_ref}"
    stats = run_git(["diff", "--stat", diff_range])
    return stats.strip()


def merge_section_into_file(section: str, output_path: Path, insert_at_top: bool) -> None:
    """Insert the release section into docs/RELEASE_NOTES.md."""
    if not output_path.exists():
        output_path.write_text("# Release Notes\n\n", encoding="utf-8")

    original = output_path.read_text(encoding="utf-8")
    if "# Release Notes" not in original:
        original = "# Release Notes\n\n" + original

    if insert_at_top:
        parts = original.splitlines()
        header = parts[0]
        remainder = "\n".join(parts[1:]).lstrip("\n")
        new_text = f"{header}\n\n{section}\n{remainder}".rstrip() + "\n"
    else:
        new_text = (original.rstrip() + "\n\n" + section).rstrip() + "\n"

    output_path.write_text(new_text, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Markdown release notes from git history."
    )
    parser.add_argument(
        "--from-ref",
        help="Base ref/commit for the comparison (e.g. origin/main).",
    )
    parser.add_argument(
        "--to-ref",
        default="HEAD",
        help="Target ref/commit for the comparison (default: HEAD).",
    )
    parser.add_argument(
        "--since",
        help="Optional git --since filter (e.g. '2025-11-01').",
    )
    parser.add_argument(
        "--title",
        help="Release title (e.g. 'Smoke-mode Improvements').",
    )
    parser.add_argument(
        "--overview",
        help="Short overview paragraph to include in the notes.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path to the release notes file to update.",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print the release notes section instead of writing to disk.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the end of the file instead of inserting after the header.",
    )
    parser.add_argument(
        "--include-merges",
        action="store_true",
        help="Include merge commits (default: merges are skipped).",
    )
    parser.add_argument(
        "--date",
        help="Release date to show in the heading (default: today, YYYY-MM-DD).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    release_date = args.date or _dt.date.today().isoformat()
    title = args.title or "Unreleased Changes"
    overview = args.overview or "Add a short narrative for stakeholders here."

    commits = collect_commits(
        from_ref=args.from_ref,
        to_ref=args.to_ref,
        since=args.since,
        include_merges=args.include_merges,
    )

    highlights = build_highlights(commits)
    table = build_commit_table(commits)
    stats = diff_stats(args.from_ref, args.to_ref)

    section_parts = [
        f"## {release_date} – {title}",
        "",
        "### Overview",
        overview,
        "",
        "### Highlights",
        highlights,
        "",
        "### Commit Details",
        table,
    ]

    if stats:
        section_parts.extend(
            [
                "",
                "### Diff Stats",
                "```\n" + stats + "\n```",
            ]
        )

    section = "\n".join(section_parts).strip() + "\n"

    if args.print_only:
        sys.stdout.write(section)
        return

    output_path = Path(args.output)
    merge_section_into_file(section, output_path, insert_at_top=not args.append)
    print(f"Wrote release notes to {output_path}")


if __name__ == "__main__":
    main()
