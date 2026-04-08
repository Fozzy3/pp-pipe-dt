"""HTTP client for 511.org GTFS-RT API."""

import logging

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.511.org/Transit"

# BOM character that 511.org prepends to responses
BOM = b"\xef\xbb\xbf"


def fetch_feed(endpoint: str, api_key: str, agency: str) -> bytes:
    """Fetch a GTFS-RT protobuf feed from 511.org.

    Args:
        endpoint: "TripUpdates" or "VehiclePositions"
        api_key: 511.org API token
        agency: Agency code (e.g. "SF" for SF Muni)

    Returns:
        Raw protobuf bytes.

    Raises:
        httpx.HTTPStatusError: On 4xx/5xx responses.
        httpx.TimeoutException: On request timeout.
    """
    url = f"{BASE_URL}/{endpoint}"
    params = {"api_key": api_key, "agency": agency}

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    content = response.content
    # 511.org sometimes prepends a UTF-8 BOM to protobuf responses
    if content.startswith(BOM):
        content = content[len(BOM) :]

    logger.debug("Fetched %s: %d bytes", endpoint, len(content))
    return content


def fetch_trip_updates(api_key: str, agency: str) -> bytes:
    return fetch_feed("TripUpdates", api_key, agency)


def fetch_vehicle_positions(api_key: str, agency: str) -> bytes:
    return fetch_feed("VehiclePositions", api_key, agency)
