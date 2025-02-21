#!/usr/bin/env python3
import asyncio, aiohttp, requests, ssl, time, json, logging, datetime, pandas as pd
import os
import threading

# ------------------ Global Timing Data ------------------
ssl_check_timings = {"handshake": [], "http": []}
web_check_timings = []
mail_check_timings = []

# ------------------ Configuration & Constants ------------------
PANEL_URL = "https://panel.businessidentity.llc"
API_KEY = "ptla_XjxL979xLjfkJ6mGhkukaNQu9qeCTg3YiE4uFrBOUpP"
REQUEST_TIMEOUT = (5, 10)  # Increased timeout
ASYNC_TOTAL_TIMEOUT = 30  # Increased overall timeout
CONCURRENCY_LIMIT = 40  # Adjust for scaling on Render
RETRY_LIMIT = 5  # Number of retries for failed requests
RETRY_BACKOFF = 2  # Exponential backoff multiplier

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

PDF_RENDER_EXPORT_URL = "http://flask-outage-app:8080/upload"  # Internal Render URL

# ------------------ Logging Setup ------------------
logging.basicConfig(
    filename="server_check.log",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ------------------ Asynchronous Functions ------------------
async def async_check_ssl_handshake(hostname, timeout=10):
    start = time.monotonic()
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
        logging.warning(f"SSL handshake error for {hostname}: {e}")
        return False
    finally:
        elapsed = time.monotonic() - start
        ssl_check_timings["handshake"].append(elapsed)

async def async_get_with_retries(url, session, retries=RETRY_LIMIT, delay=1, ssl_option=False):
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, ssl=ssl_option) as resp:
                if 200 <= resp.status < 400:
                    return resp.status
                else:
                    logging.warning(f"Attempt {attempt}: {url} returned status {resp.status}")
        except Exception as e:
            logging.warning(f"Attempt {attempt}: Error fetching {url}: {e}")
        await asyncio.sleep(delay * (RETRY_BACKOFF ** (attempt - 1)))
    return None

async def check_server_async(server, session, node_map):
    server_name = server["attributes"]["name"]
    node_id = server["attributes"]["node"]
    node_name = node_map.get(node_id, f"Node {node_id}")
    external_identifier = server["attributes"].get("external_id")

    if not external_identifier:
        return (server_name, node_name, False, False, False)

    server_url = f"https://{external_identifier}"
    mail_url = f"https://mail.{external_identifier}"

    web_status = await async_get_with_retries(server_url, session, retries=RETRY_LIMIT)
    mail_status = await async_get_with_retries(mail_url, session, retries=RETRY_LIMIT)
    
    is_up = web_status is not None
    mail_up = mail_status is not None
    ssl_valid = await async_check_ssl_handshake(external_identifier)

    return (server_name, node_name, is_up, mail_up, ssl_valid)

# ------------------ Synchronous Helper Functions ------------------
def get_all_nodes():
    logging.info("Fetching node list from API...")
    try:
        response = requests.get(f"{PANEL_URL}/api/application/nodes", headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        nodes = response.json().get("data", [])
        return {node["attributes"]["id"]: node["attributes"]["name"] for node in nodes}
    except requests.RequestException as e:
        logging.error(f"Failed to fetch nodes: {e}")
        return {}

def get_all_servers():
    logging.info("Fetching server list from API...")
    servers = []
    page = 1
    while True:
        try:
            response = requests.get(f"{PANEL_URL}/api/application/servers?page={page}", headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json().get("data", [])
            if not data:
                break
            servers.extend(data)
            page += 1
        except requests.RequestException as e:
            logging.error(f"Failed to fetch servers: {e}")
            break
    return servers

async def process_all_checks(servers, node_map):
    results = []
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    timeout = aiohttp.ClientTimeout(total=ASYNC_TOTAL_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [asyncio.create_task(sem_task(server, session, node_map, sem)) for server in servers]
        for task in asyncio.as_completed(tasks):
            result = await task
            results.append(result)
    return results

async def sem_task(server, session, node_map, sem):
    async with sem:
        return await check_server_async(server, session, node_map)

# ------------------ Export Functions ------------------
def export_results_to_excel(server_status):
    filename = f"server_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df = pd.DataFrame(server_status, columns=["Server Name", "Node Name", "Web Status", "Mail Status", "SSL"])
    df.to_excel(filename, index=False)
    logging.info(f"Results exported to {filename}")
    return filename

def export_pdf_file_to_remote(pdf_filename):
    try:
        with open(pdf_filename, 'rb') as f:
            files = {'file': (pdf_filename, f, 'application/pdf')}
            response = requests.post(PDF_RENDER_EXPORT_URL, files=files)
            if response.status_code == 200:
                logging.info(f"PDF successfully uploaded: {pdf_filename}")
            else:
                logging.error(f"Failed to upload PDF: {response.status_code}")
    except Exception as e:
        logging.error(f"Error uploading PDF: {e}")

# ------------------ Main Routine ------------------
def run_server_check():
    logging.info("Starting server check process.")
    node_map = get_all_nodes()
    servers = get_all_servers()
    if not servers:
        logging.error("No servers found to check.")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(process_all_checks(servers, node_map))
    loop.close()

    formatted_results = [
        (server, node, "UP" if is_up else "DOWN", "UP" if mail_up else "DOWN", "YES" if ssl else "NO")
        for server, node, is_up, mail_up, ssl in results
    ]

    excel_file = export_results_to_excel(formatted_results)
    logging.info("Server check completed successfully.")

# ------------------ Entry Point for Render Background Worker ------------------
if __name__ == "__main__":
    run_server_check()
