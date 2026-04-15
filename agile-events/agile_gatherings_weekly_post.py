from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

SEARCH_RESULTS_URL = "https://agilegatherings.com/search-results/"
SECTION_BREAK = "====================================="
USER_AGENT = "Mozilla/5.0 (compatible; AgileGatheringsWeeklyPost/0.1)"
EVENT_PAGE_STOP_HEADINGS = {
    "share this event",
    "event details",
    "related events",
    "contact",
    "sign up for our newsletter",
    "featured events",
    "latest guest blogs",
    "lastest guest blogs",
    "register",
    "add to calendar",
}


COUNTRY_TO_ISO = {
    "Argentina": "AR",
    "Armenia": "AM",
    "Australia": "AU",
    "Austria": "AT",
    "Azerbaijan": "AZ",
    "Bangladesh": "BD",
    "Belgium": "BE",
    "Bosnia": "BA",
    "Brasil": "BR",
    "Brazil": "BR",
    "Bulgaria": "BG",
    "Canada": "CA",
    "Chile": "CL",
    "China": "CN",
    "Colombia": "CO",
    "Costa Rica": "CR",
    "Czech Republic": "CZ",
    "Denmark": "DK",
    "Ecuador": "EC",
    "Egypt": "EG",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Ghana": "GH",
    "Greece": "GR",
    "Guatemala": "GT",
    "Hungary": "HU",
    "India": "IN",
    "Indonesia": "ID",
    "Ireland": "IE",
    "Israel": "IL",
    "Italy": "IT",
    "Japan": "JP",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Malaysia": "MY",
    "Nepal": "NP",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Nigeria": "NG",
    "Norway": "NO",
    "Pakistan": "PK",
    "Peru": "PE",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Serbia": "RS",
    "Singapore": "SG",
    "Slovakia": "SK",
    "Slovenia": "SI",
    "South Korea": "KR",
    "South-Africa": "ZA",
    "Spain": "ES",
    "Sri Lanka": "LK",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Taiwan": "TW",
    "Thailand": "TH",
    "The United Arab Emirates": "AE",
    "Turkey": "TR",
    "United Kingdom": "GB",
    "USA": "US",
    "Uzbekistan": "UZ",
}


@dataclass(frozen=True)
class Event:
    title: str
    url: str
    start_date: date
    end_date: date
    location_text: str | None = None
    mode: str | None = None
    location_parts: tuple[str, ...] = field(default_factory=tuple)
    description: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a weekly Agile Gatherings LinkedIn post from the search results page."
    )
    parser.add_argument(
        "--mode",
        choices=("next_week", "this_week"),
        default="next_week",
        help="Date window mode. Ignored when --start and --end are provided.",
    )
    parser.add_argument("--start", help="Manual start date in YYYY-MM-DD format.")
    parser.add_argument("--end", help="Manual end date in YYYY-MM-DD format.")
    args = parser.parse_args()

    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be provided together.")

    return args


def compute_date_window(args: argparse.Namespace, today: date | None = None) -> tuple[date, date]:
    today = today or date.today()
    if args.start and args.end:
        start_date = parse_cli_date(args.start)
        end_date = parse_cli_date(args.end)
        if end_date < start_date:
            raise ValueError("--end must be on or after --start.")
        return start_date, end_date

    if args.mode == "this_week":
        return this_week_window(today)
    return next_week_window(today)


def parse_cli_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def next_week_window(today: date) -> tuple[date, date]:
    # "Next week" always starts on the next Sunday, even if today is Sunday.
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        days_until_sunday = 7
    window_start = today + timedelta(days=days_until_sunday)
    return window_start, window_start + timedelta(days=6)


def this_week_window(today: date) -> tuple[date, date]:
    days_since_sunday = (today.weekday() + 1) % 7
    window_start = today - timedelta(days=days_since_sunday)
    return window_start, window_start + timedelta(days=6)


def fetch_page_html(url: str) -> str:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        raise RuntimeError(f"Could not fetch Agile Gatherings page from {url}: {exc}") from exc


def parse_event_cards(html: str) -> list[Event]:
    soup = BeautifulSoup(html, "html.parser")
    cards = find_event_cards(soup)

    events: list[Event] = []
    seen: set[tuple[str, str, str, str]] = set()

    for card in cards:
        try:
            event = parse_event_card(card)
        except Exception as exc:
            title = (
                clean_text(card.select_one(".gt-title").get_text(" ", strip=True))
                if card.select_one(".gt-title")
                else "Unknown title"
            )
            print(f"Warning: skipped event card '{title}': {exc}", file=sys.stderr)
            continue

        key = event_dedup_key(event)
        if key in seen:
            continue
        seen.add(key)
        events.append(event)

    return events


def parse_event_card(card: Tag) -> Event:
    # Search result cards already include the data needed for this MVP.
    title_link = card.select_one(".gt-title a")
    if not title_link:
        raise ValueError("missing title link")

    title = clean_text(title_link.get_text(" ", strip=True))
    href = title_link.get("href")
    if not href:
        raise ValueError("missing event URL")
    url = urljoin(SEARCH_RESULTS_URL, href)

    start_text = text_or_none(card.select_one(".gt-start-date span"))
    if not start_text:
        raise ValueError("missing start date")
    end_text = text_or_none(card.select_one(".gt-end-date span")) or start_text

    start_date = normalize_date(start_text)
    end_date = normalize_date(end_text)
    if end_date < start_date:
        # Keep obviously broken ranges from creating false overlap matches.
        end_date = start_date

    location_parts = parse_location_parts(card)
    location_text = ", ".join(location_parts) if location_parts else None
    mode = text_or_none(card.select_one(".gt-image .gt-label span"))

    return Event(
        title=title,
        url=url,
        start_date=start_date,
        end_date=end_date,
        location_text=location_text,
        mode=mode,
        location_parts=location_parts,
    )


def normalize_date(value: str) -> date:
    return datetime.strptime(clean_text(value), "%d %b %y").date()


def find_event_cards(soup: BeautifulSoup) -> list[Tag]:
    container = soup.select_one(".gt-event-search-results")
    if container:
        return list(container.select(".gt-event-style-1"))
    return list(soup.select(".gt-event-style-1"))


def event_dedup_key(event: Event) -> tuple[str, str, str, str]:
    return (
        event.title.casefold(),
        event.url,
        event.start_date.isoformat(),
        event.end_date.isoformat(),
    )


def parse_location_parts(card: Tag) -> tuple[str, ...]:
    parts: list[str] = []
    for item in card.select(".gt-location li"):
        text = clean_text(item.get_text(" ", strip=True))
        if text:
            parts.append(text)
    return tuple(parts)


def filter_events(events: Iterable[Event], window_start: date, window_end: date) -> list[Event]:
    matching = [
        event
        for event in events
        if event.start_date <= window_end and event.end_date >= window_start
    ]
    return sorted(matching, key=lambda event: (event.start_date, event.title.casefold()))


def enrich_event_descriptions(events: Iterable[Event]) -> list[Event]:
    enriched_events: list[Event] = []
    for event in events:
        try:
            html = fetch_page_html(event.url)
        except RuntimeError as exc:
            print(f"Warning: could not fetch event page for '{event.title}': {exc}", file=sys.stderr)
            enriched_events.append(event)
            continue

        description = extract_event_description(html)
        enriched_events.append(replace(event, description=description))
    return enriched_events


def extract_event_description(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1")
    if title_tag:
        for element in title_tag.find_all_next(["h2", "h3", "h4", "p", "div"]):
            text = clean_text(element.get_text(" ", strip=True))
            if not text:
                continue
            if element.name in {"h2", "h3", "h4"}:
                if normalize_heading(text) in EVENT_PAGE_STOP_HEADINGS:
                    break
                continue
            if element.name == "div" and element.find(["p", "div", "h2", "h3", "h4"]):
                continue
            if is_noise_description_text(text, title_tag):
                continue
            return trim_description(text)

    return extract_meta_description(soup)


def normalize_heading(text: str) -> str:
    return clean_text(text).casefold().rstrip(":")


def is_noise_description_text(text: str, title_tag: Tag) -> bool:
    title_text = clean_text(title_tag.get_text(" ", strip=True))
    if text == title_text:
        return True
    if normalize_heading(text) in EVENT_PAGE_STOP_HEADINGS:
        return True
    if len(text) < 40:
        return True
    if re.fullmatch(r"[\W_]+", text):
        return True
    return False


def trim_description(text: str, max_chars: int = 300) -> str:
    text = clean_text(text)
    if len(text) <= max_chars:
        return text

    sentences = re.split(r"(?<=[.!?])\s+", text)
    excerpt = ""
    for sentence in sentences:
        candidate = f"{excerpt} {sentence}".strip()
        if len(candidate) > max_chars:
            break
        excerpt = candidate

    if excerpt and len(excerpt) >= 80:
        return excerpt

    shortened = text[: max_chars + 1]
    if " " in shortened:
        shortened = shortened.rsplit(" ", 1)[0]
    return shortened.rstrip(" ,;:") + "..."


def extract_meta_description(soup: BeautifulSoup) -> str | None:
    for selector in (
        'meta[property="og:description"]',
        'meta[name="description"]',
    ):
        tag = soup.select_one(selector)
        if not tag:
            continue
        content = clean_text(tag.get("content", ""))
        if content:
            return trim_description(content)
    return None


def format_linkedin_post(events: list[Event], window_start: date, window_end: date) -> str:
    window_label = format_window_label(window_start, window_end)
    event_blocks = "\n\n".join(format_event_block(event) for event in events)

    return (
        f"🌍 Agile Gatherings | {window_label}\n\n"
        "Wherever you are in the world, something is happening this week that brings Agile people together.\n\n"
        "Some events are big. Some are small. Some are online, others in person. But they all have one thing in common. "
        "People showing up to learn, share, and figure things out together.\n\n"
        "Agile Gatherings is excited to present what's coming up:\n\n"
        f"{event_blocks}\n\n"
        "No matter which one you join, these gatherings are really about the people in the room. The conversations, the shared "
        "stories, and those small moments where things click.\n\n"
        "If one of these is on your radar, we'd love to hear 👇\n"
        "We'll drop the links in the comments so you can explore each one.\n\n"
        "#AgileGatherings #AgileCommunity #ContinuousLearning #GlobalAgile\n\n"
        "⸻⸻⸻\n"
        "👋 There’s more ahead\n\n"
        "➕ Follow Agile Gatherings for:\n"
        "⚪️ Upcoming Agile events\n"
        "⚪️ Community updates from around the world\n"
        "⚪️ Ways to learn, share, and get involved\n\n"
        "Welcome to the community.\n"
        "⸻⸻⸻"
    )


def format_event_block(event: Event) -> str:
    lines = [
        format_event_heading(event),
        f"📅 {format_event_date(event.start_date, event.end_date)}",
        f"📍 {event_location_label(event)}",
    ]
    if event.description:
        lines.append(event.description)
    return "\n".join(lines)


def format_first_comment(events: list[Event]) -> str:
    blocks = [format_comment_event_block(event) for event in events]
    return (
        "Explore these gatherings and learn more 👇\n"
        "Start with our events calendar:\n\n"
        "https://agilegatherings.com\n\n"
        + "\n\n".join(blocks)
    )


def format_event_heading(event: Event) -> str:
    # Future extension point:
    # Add host / gathering mention logic here once that metadata is available.
    return f"{event_icon(event)} {event.title}"


def format_comment_event_block(event: Event) -> str:
    return "\n".join([format_event_heading(event), event.url])


def event_location_label(event: Event) -> str:
    if event.location_text:
        return event.location_text
    if event.mode:
        return event.mode
    return "Location TBA"


def event_icon(event: Event) -> str:
    if (event.mode or "").lower() == "online":
        return "🌐"

    for location_part in event.location_parts:
        if location_part == "Online":
            continue
        iso_code = COUNTRY_TO_ISO.get(location_part)
        if iso_code:
            return iso_to_flag(iso_code)

    return "🌐"


def iso_to_flag(iso_code: str) -> str:
    return "".join(chr(127397 + ord(char)) for char in iso_code.upper())


def format_event_date(start_date: date, end_date: date) -> str:
    if start_date == end_date:
        return format_full_date(start_date)
    if start_date.year == end_date.year and start_date.month == end_date.month:
        return f"{format_month_day(start_date)}–{end_date.day}, {end_date.year}"
    if start_date.year == end_date.year:
        return f"{format_month_day(start_date)}–{format_month_day(end_date)}, {end_date.year}"
    return f"{format_full_date(start_date)}–{format_full_date(end_date)}"


def format_window_label(start_date: date, end_date: date) -> str:
    return format_event_date(start_date, end_date)


def print_section(title: str, body: str) -> None:
    print(SECTION_BREAK)
    print(title)
    print(SECTION_BREAK)
    print()
    print(body)
    print()


def print_found_output(events: list[Event], window_start: date, window_end: date) -> None:
    window_label = format_window_label(window_start, window_end)
    print_section("STATUS", f"Found {len(events)} events for {window_label}")
    print_section("LINKEDIN POST", format_linkedin_post(events, window_start, window_end))
    print_section("FIRST COMMENT", format_first_comment(events))


def print_no_events_output(window_start: date, window_end: date) -> None:
    window_label = format_window_label(window_start, window_end)
    print_section("STATUS", f"Found 0 events for {window_label}")
    print_section("NO EVENTS FOUND", build_no_events_message())


def print_error_output(message: str) -> None:
    print_section("STATUS", "Unable to build the Agile Gatherings weekly post.")
    print_section("ERROR", message)


def text_or_none(tag: Tag | None) -> str | None:
    if tag is None:
        return None
    text = clean_text(tag.get_text(" ", strip=True))
    return text or None


def clean_text(value: str) -> str:
    return " ".join(value.split())


def build_no_events_message() -> str:
    return (
        "No Agile Gatherings events are currently listed for this date range.\n\n"
        "Suggested next step:\n"
        "Create an alternate LinkedIn post highlighting another part of the Agile Gatherings site or community."
    )


def format_month_day(value: date) -> str:
    return f"{value.strftime('%B')} {value.day}"


def format_full_date(value: date) -> str:
    return f"{format_month_day(value)}, {value.year}"


def main() -> int:
    args = parse_args()

    try:
        window_start, window_end = compute_date_window(args)
        html = fetch_page_html(SEARCH_RESULTS_URL)
        events = parse_event_cards(html)
        # Future extension point:
        # Add Notion lookup integration here if you want to enrich events with
        # host/community metadata before formatting the post and comment.
        matching_events = enrich_event_descriptions(filter_events(events, window_start, window_end))
    except ValueError as exc:
        print_error_output(str(exc))
        return 2
    except RuntimeError as exc:
        print_error_output(str(exc))
        return 1

    if matching_events:
        print_found_output(matching_events, window_start, window_end)
    else:
        print_no_events_output(window_start, window_end)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
