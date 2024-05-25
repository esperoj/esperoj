import time
import requests
from requests.adapters import HTTPAdapter, Retry
import os


def save_page(url: str) -> str:
    """Archive a URL using the Save Page Now 2 (SPN2) API.

    Args:
        url (str): The URL to archive.

    Returns:
        str: The archived URL.

    Raises:
        RuntimeError: If the URL cannot be archived or if a timeout occurs.
    """
    api_key = os.environ.get("INTERNET_ARCHIVE_ACCESS_KEY")
    api_secret = os.environ.get("INTERNET_ARCHIVE_SECRET_KEY")

    headers = {
        "Accept": "application/json",
        "Authorization": f"LOW {api_key}:{api_secret}",
    }

    params = {
        "url": url,
        "capture_all": 0,
        "capture_outlinks": 0,
        "capture_screenshot": 0,
        "delay_wb_availability": 0,
        "force_get": 0,
        "skip_first_archive": 1,
        "outlinks_availability": 0,
        "email_result": 1,
        "js_behavior_timeout": 30,
    }

    session = requests.Session()

    retry_strategy = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.post(
        "https://web.archive.org/save", headers=headers, data=params, timeout=12
    )
    if response.status_code != 200:
        raise RuntimeError(f"Error: {response.text}")
    job_id = response.json()["job_id"]

    start_time = time.time()
    timeout = 60 * 15

    while True:
        if time.time() - start_time > timeout:
            raise RuntimeError("Error: Archiving process timed out.")
        response = session.get(
            f"https://web.archive.org/save/status/{job_id}", headers=headers, timeout=12
        )
        if response.status_code != 200:
            raise RuntimeError(f"Error: {response.text}")
        status = response.json()
        match status["status"]:
            case "pending":
                time.sleep(5)
            case "success":
                return f'https://web.archive.org/web/{status["timestamp"]}/{status["original_url"]}'
            case _:
                raise RuntimeError(f"Error: {response.text}")


def get_esperoj_method(esperoj):
    """Get the method to archive URLs using the Save Page Now 2 (SPN2) API.

    Args:
        esperoj (object): An object representing the esperoj archive.

    Returns:
        function: The method to archive URLs using the SPN2 API.
    """
    return save_page


def get_click_command():
    """Get the Click command to archive a URL using the Save Page Now 2 (SPN2) API.

    Returns:
        click.Command: The Click command to archive a URL using the SPN2 API.
    """
    import click

    @click.command()
    @click.argument("url", type=click.STRING, required=True)
    def click_command(url):
        """Archive a URL using the Save Page Now 2 (SPN2) API.

        Args:
            url (str): The URL to archive.
        """
        print(save_page(url))

    return click_command
