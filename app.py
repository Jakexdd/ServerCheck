import asyncio
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import requests
import pandas as pd
import aiohttp
import ssl
import os
import time
import asyncio

# ------------------ Configuration ------------------
PANEL_URL = "https://panel.businessidentity.llc"
API_KEY = "ptla_XjxL979xLjfkJ6mGhkukaNQu9qeCTg3YiE4uFrBOUpP"
REQUEST_TIMEOUT = (10, 10)
CONCURRENCY_LIMIT = 10
PDF_RENDER_EXPORT_URL = "https://flask-outage-app:8080/upload"

# ------------------ Logging Setup ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# ------------------ Async Server Checks ------------------
async def async_check_ssl_handshake(hostname, timeout=10):
    """Check SSL handshake status."""
    try:
        ssl_context = ssl.create_default_context()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(hostname, 443, ssl=ssl_context, server_hostname=hostname),
            timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception as e:
        logging.error(f"SSL handshake failed for {hostname}: {e}")
        return False

async def async_get_with_retries(url, session, retries=3, delay=1):
    """Perform GET request with retries."""
    for attempt in range(retries):
        try:
            async with session.get(url, ssl=False) as resp:
                if 200 <= resp.status < 400:
                    return resp.status
        except Exception as e:
            logging.warning(f"Retry {attempt+1} failed for {url}: {e}")
        await asyncio.sleep(delay)
    return None

async def check_server_async(server, session, node_map):
    """Check server status asynchronously."""
    server_name = server["attributes"]["name"]
    external_id = server["attributes"].get("external_id")
    node_id = server["attributes"]["node"]
    node_name = node_map.get(node_id, f"Node {node_id}")

    if not external_id:
        return (server_name, node_name, False, False, False)

    server_url = f"https://{external_id}"
    mail_url = f"https://mail.{external_id}"

    web_status, mail_status = await asyncio.gather(
        async_get_with_retries(server_url, session),
        async_get_with_retries(mail_url, session)
    )

    ssl_status = await async_check_ssl_handshake(external_id)

    return (server_name, node_name, bool(web_status), bool(mail_status), ssl_status)

# ------------------ API Fetching ------------------
def get_all_nodes():
    """Get all nodes from the API."""
    logging.info("Fetching nodes from API...")
    try:
        response = requests.get(f"{PANEL_URL}/api/application/nodes", headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        nodes = response.json().get("data", [])
        return {node["attributes"]["id"]: node["attributes"]["name"] for node in nodes}
    except requests.RequestException as e:
        logging.error(f"Error fetching nodes: {e}")
        return {}

def get_all_servers():
    """Get all servers from the API."""
    logging.info("Fetching servers from API...")
    servers = []
    page = 1
    while True:
        try:
            response = requests.get(
                f"{PANEL_URL}/api/application/servers?page={page}&per_page=1000",
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json().get("data", [])
            if not data:
                break
            servers.extend(data)
            page += 1
        except requests.RequestException as e:
            logging.error(f"Error fetching servers: {e}")
            break
    return servers

# ------------------ Report Generation ------------------
def generate_report(results):
    """Generate a report and save it as an Excel file."""
    now = datetime.datetime.now()
    filename = f"server_status_{now.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    df = pd.DataFrame(results, columns=["Server Name", "Node Name", "Web Status", "Mail Status", "SSL Valid"])
    df.to_excel(filename, index=False)
    logging.info(f"Report saved to {filename}")
    return filename

def upload_report_to_pdf_service(excel_file):
    """Upload the Excel report to the PDF processing service."""
    try:
        with open(excel_file, 'rb') as f:
            response = requests.post(PDF_RENDER_EXPORT_URL, files={'file': f})
        if response.status_code == 200:
            logging.info("Report successfully uploaded to the PDF service.")
        else:
            logging.error(f"Failed to upload report. Status: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to upload report: {e}")

# ------------------ Main Task Runner ------------------
async def run_server_check():
    """Main server check task."""
    logging.info("Starting full server check...")
    node_map = get_all_nodes()
    servers = get_all_servers()

    results = []
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [check_server_async(server, session, node_map) for server in servers]
        for future in asyncio.as_completed(tasks):
            result = await future
            results.append(result)

    report_file = generate_report(results)
    upload_report_to_pdf_service(report_file)
    logging.info("Server check completed successfully.")

# ------------------ Scheduler ------------------
scheduler = BlockingScheduler()

# Schedule the task to run daily at 7:30 AM PST (15:30 UTC)
scheduler.add_job(lambda: asyncio.run(run_server_check()), 'cron', hour=15, minute=30, timezone='UTC')

# ------------------ Entry Point ------------------
import asyncio

# ✅ Corrected main execution block for async execution
if __name__ == "__main__":
    logging.info("Starting background worker...")

    # Properly run the async function immediately after deployment
    asyncio.run(run_server_check())

    # Schedule daily job at 7:30 AM PST
    scheduler.add_job(
        lambda: asyncio.run(run_server_check()),  # Ensures async runs properly
        trigger="cron",
        hour=7,
        minute=30,
        timezone="US/Pacific"
    )

    scheduler.start()
