#!/usr/bin/env python3
import asyncio, aiohttp, requests, ssl, time, json, logging, datetime, pandas as pd, threading
from flask import Flask, jsonify

# ------------------ Global Timing Data ------------------
ssl_check_timings = {"handshake": [], "http": []}
web_check_timings = []
mail_check_timings = []

# ------------------ Configuration & Constants ------------------
PANEL_URL = "https://panel.businessidentity.llc"
API_KEY = "ptla_XjxL979xLjfkJ6mGhkukaNQu9qeCTg3YiE4uFrBOUpP"
REQUEST_TIMEOUT = (3, 3)
ASYNC_TOTAL_TIMEOUT = 10
CONCURRENCY_LIMIT = 40

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Set this to your Render server’s endpoint that will accept the Excel file.
RENDER_EXPORT_URL = "https://your-render-server-url/upload"  # Modify accordingly

logging.basicConfig(
    filename="server_check.log",
    filemode="a",
    level=logging.DEBUG,
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
        try:
            await writer.wait_closed()
        except AttributeError:
            pass
        return True
    except Exception as e:
        logging.exception(f"Async SSL handshake error for {hostname}: {e}")
        return False
    finally:
        elapsed = time.monotonic() - start
        ssl_check_timings["handshake"].append(elapsed)

async def async_check_ssl_http(hostname, timeout=10, session=None):
    start = time.monotonic()
    try:
        if session is None:
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.get(f"https://{hostname}", ssl=True) as resp:
                    return 200 <= resp.status < 400
        else:
            async with session.get(f"https://{hostname}", ssl=True) as resp:
                return 200 <= resp.status < 400
    except Exception as e:
        logging.exception(f"Async HTTP SSL check error for {hostname}: {e}")
        return False
    finally:
        elapsed = time.monotonic() - start
        ssl_check_timings["http"].append(elapsed)

async def async_check_ssl_certificate(hostname, retries=3, delay=1, timeout=10, session=None):
    for _ in range(retries):
        tasks = [
            async_check_ssl_handshake(hostname, timeout),
            async_check_ssl_http(hostname, timeout, session)
        ]
        results = await asyncio.gather(*tasks)
        if any(results):
            return True
        await asyncio.sleep(delay)
    return False

async def async_get_with_retries(url, session, retries=3, delay=1, ssl_option=False):
    for attempt in range(retries):
        try:
            async with session.get(url, ssl=ssl_option) as resp:
                if 200 <= resp.status < 400:
                    return resp.status
                else:
                    logging.warning(f"Attempt {attempt+1}: {url} returned status {resp.status}")
        except Exception as e:
            logging.exception(f"Attempt {attempt+1}: Error fetching {url}: {e}")
        await asyncio.sleep(delay)
    return None

async def timed_async_get_with_retries(url, session, retries=3, delay=1, ssl_option=False, label=""):
    start = time.monotonic()
    result = await async_get_with_retries(url, session, retries, delay, ssl_option)
    elapsed = time.monotonic() - start
    if label == "web":
        web_check_timings.append(elapsed)
    elif label == "mail":
        mail_check_timings.append(elapsed)
    return result

async def check_server_async(server, session, node_map, pause_event=None):
    server_name = server["attributes"]["name"]
    server_uuid = server["attributes"]["uuid"]
    node_id = server["attributes"]["node"]
    node_name = node_map.get(node_id, f"Node {node_id}")
    external_identifier = server["attributes"].get("external_id")
    if not external_identifier:
        return (server_uuid, node_name, server_name, False, False, False)
    server_url = f"https://{external_identifier}"
    mail_url = f"https://mail.{external_identifier}"
    is_up_status, mail_status = await asyncio.gather(
        timed_async_get_with_retries(server_url, session, retries=3, delay=1, ssl_option=False, label="web"),
        timed_async_get_with_retries(mail_url, session, retries=3, delay=1, ssl_option=False, label="mail")
    )
    is_up = is_up_status is not None
    mail_up = mail_status is not None
    try:
        ssl_results = await asyncio.gather(
            async_check_ssl_certificate(external_identifier, retries=3, delay=1, timeout=10, session=session),
            async_check_ssl_certificate("mail." + external_identifier, retries=3, delay=1, timeout=10, session=session)
        )
        ssl_valid = ssl_results[0] or ssl_results[1]
    except Exception as e:
        logging.exception(f"Error checking SSL for {external_identifier}: {e}")
        ssl_valid = False
    return (server_uuid, node_name, server_name, is_up, mail_up, ssl_valid)

# ------------------ Synchronous Functions ------------------
def get_all_servers():
    logging.info("Fetching full server list from API...")
    servers = []
    page = 1
    try:
        while True:
            url = f"{PANEL_URL}/api/application/servers?page={page}&per_page=1000"
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                logging.error(f"API Error: {response.status_code}")
                break
            data = response.json()
            page_servers = data.get("data", [])
            if not page_servers:
                break
            servers.extend(page_servers)
            logging.info(f"Retrieved {len(servers)} servers so far...")
            page += 1
        return servers
    except Exception as e:
        logging.exception(f"Error fetching servers: {e}")
        return servers

def get_all_nodes():
    logging.info("Fetching nodes list...")
    nodes = []
    page = 1
    try:
        while True:
            url = f"{PANEL_URL}/api/application/nodes?page={page}"
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                logging.error(f"API Error fetching nodes: {response.status_code}")
                break
            data = response.json()
            page_nodes = data.get("data", [])
            if not page_nodes:
                break
            nodes.extend(page_nodes)
            logging.info(f"Retrieved {len(nodes)} nodes so far...")
            page += 1
        logging.info(f"Total nodes fetched: {len(nodes)}")
        node_map = {}
        for node in nodes:
            attributes = node.get("attributes", {})
            nid = attributes.get("id")
            name = attributes.get("name")
            if nid is not None and name is not None:
                node_map[nid] = name
        return node_map
    except Exception as e:
        logging.exception(f"Error fetching nodes: {e}")
        return {}

async def process_all_checks(servers, node_map):
    results = []
    sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
    timeout = aiohttp.ClientTimeout(total=ASYNC_TOTAL_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def sem_task(server):
            async with sem:
                return await check_server_async(server, session, node_map)
        tasks = [asyncio.create_task(sem_task(s)) for s in servers]
        for task in asyncio.as_completed(tasks):
            result = await task
            results.append(result)
    return results

def export_results_to_excel(server_status):
    try:
        dt = datetime.datetime.now()
        checked_at = dt.strftime("%B %d %Y %I:%M:%S %p")
        filename_timestamp = dt.strftime("%B_%d_%Y_%I-%M-%S_%p")
        filename = f"results_{filename_timestamp}.xlsx"
        df = pd.DataFrame([
            {
                "UUID": uuid,
                "Server Name": details.get("server_name", ""),
                "Node Name": details.get("node_name", ""),
                "Web Status": "UP" if details.get("is_up", False) else "DOWN",
                "Mail Status": "UP" if details.get("mail_up", False) else "DOWN",
                "SSL": "Yes" if details.get("ssl_valid", False) else "No",
                "Checked At": checked_at
            }
            for uuid, details in server_status.items()
        ])
        df.to_excel(filename, index=False)
        logging.info(f"Results exported to {filename}")
        return filename
    except Exception as e:
        logging.exception("Error exporting to Excel")
        return None

def export_file_to_remote(filename):
    try:
        with open(filename, 'rb') as f:
            files = {'file': (filename, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            response = requests.post(RENDER_EXPORT_URL, files=files)
            if response.status_code == 200:
                logging.info(f"File {filename} successfully uploaded to remote server.")
            else:
                logging.error(f"Failed to upload file {filename} to remote server. Status: {response.status_code}")
    except Exception as e:
        logging.exception("Error exporting file to remote server.")

def run_server_check():
    logging.info("Starting server check.")
    node_map = get_all_nodes()
    servers = get_all_servers()
    valid_ids = {str(k) for k in node_map.keys()}
    filtered_servers = [s for s in servers if str(s["attributes"].get("node")) in valid_ids]
    logging.info(f"Total servers to check: {len(filtered_servers)}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(process_all_checks(filtered_servers, node_map))
    loop.close()
    server_status = {}
    for res in results:
        uuid, node_name, server_name, is_up, mail_up, ssl_valid = res
        server_status[uuid] = {
            "server_name": server_name,
            "node_name": node_name,
            "is_up": is_up,
            "mail_up": mail_up,
            "ssl_valid": ssl_valid
        }
    filename = export_results_to_excel(server_status)
    if filename:
        export_file_to_remote(filename)
    return server_status, filename

# ------------------ Flask Application ------------------
app = Flask(__name__)
check_status = {
    "running": False,
    "result": None,
    "filename": None,
    "message": ""
}

def background_check():
    check_status["running"] = True
    check_status["message"] = "Server check in progress..."
    try:
        result, filename = run_server_check()
        check_status["result"] = result
        check_status["filename"] = filename
        check_status["message"] = "Server check completed."
    except Exception as e:
        check_status["message"] = f"Error during server check: {e}"
        logging.exception("Error in background server check")
    finally:
        check_status["running"] = False

@app.before_first_request
def start_background_check():
    thread = threading.Thread(target=background_check)
    thread.start()

@app.route("/")
def index():
    return "Server Check Flask App is running. Visit /status to view the current status."

@app.route("/status")
def status():
    return jsonify(check_status)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
