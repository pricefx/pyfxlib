#!/usr/bin/env python3
"""Check that all packages in poetry.lock are at least N days old.

Usage: poetry run python scripts/check_lock_freshness.py [--days N]
"""

import argparse
import asyncio
from datetime import datetime, timedelta, UTC
import sys
import tomllib

import httpx

THRESHOLD_DAYS = 7
IGNORE_ON_NULL = 0
MAX_CONCURRENCY = 20


async def fetch_publish_date(client: httpx.AsyncClient, name: str, version: str) -> datetime | None:
    """Return the earliest upload datetime for a given package version on PyPI.

    Returns None if the package/version is not found or the request fails.
    """
    base_version = version.split("+")[0]
    url = f"https://pypi.org/pypi/{name}/{base_version}/json"
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        upload_times = [
            datetime.fromisoformat(f["upload_time"]).replace(tzinfo=UTC) for f in r.json()["urls"]
        ]
        return min(upload_times)
    except Exception as e:
        print(f"  ⚠  {name}=={version}: PyPI error ({e})", file=sys.stderr)
        return None


async def safety_check(
    threshold: timedelta, ignore_on_null: bool, safe_packages: list[str]
) -> bool:
    """Check that every package in poetry.lock is older than *threshold*.

    Args:
        threshold: Minimum required age (e.g. timedelta(days=7)).
        ignore_on_null: If True, packages whose publish date cannot be fetched
            are treated as old enough. If False (default), they are flagged.
        safe_packages: Package names to skip entirely (printed as a warning).

    Returns:
        True if all checked packages meet the age requirement, False otherwise.
    """
    with open("poetry.lock", "rb") as f:
        packages = tomllib.load(f)["package"]

    if len(safe_packages) > 0:
        print(
            "\nCaution: the following packages were excluded from the check and may be fresher "
            + f"than {threshold.days} days: {', '.join(safe_packages)}"
        )

    to_check = [p for p in packages if p["name"] not in safe_packages]
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def bounded(client: httpx.AsyncClient, name: str, version: str) -> datetime | None:
        async with semaphore:
            return await fetch_publish_date(client, name, version)

    async with httpx.AsyncClient() as client:
        # asyncio.gather preserves input order, so publications[i] matches to_check[i].
        publications = await asyncio.gather(
            *(bounded(client, p["name"], p["version"]) for p in to_check)
        )

    too_fresh = []
    for package, publication in zip(to_check, publications):
        name, version = package["name"], package["version"]
        old_enough = datetime.now(UTC) - publication >= threshold if publication else ignore_on_null
        if not old_enough:
            too_fresh.append((name, version, publication))

    if len(too_fresh) > 0:
        print(f"\n{len(too_fresh)} package(s) fresher than {threshold.days} days:")
        for name, version, publication in too_fresh:
            date_str = publication.date() if publication else "unknown publish date"
            print(f"  - {name}=={version}  ({date_str})")
        return False

    print(f"\nAll packages are at least {threshold.days} days old.")
    return True


def main() -> None:
    """Parse CLI arguments and run the lock freshness check."""
    parser = argparse.ArgumentParser(description="Check lock freshness.")
    parser.add_argument("--days", type=int, default=THRESHOLD_DAYS, help="Minimum age in days.")
    parser.add_argument(
        "--ignore-on-null",
        action="store_true",
        help="Consider packages with unknown publish date as safe (default: unsafe).",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=[],
        help="Package names to exclude from the check.",
    )
    args = parser.parse_args()
    result = asyncio.run(safety_check(timedelta(days=args.days), args.ignore_on_null, args.exclude))
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
