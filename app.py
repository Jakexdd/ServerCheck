import asyncio
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import requests
import pandas as pd
import httpx
import ssl
import os
import time
import gc
import uvloop

# Set uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# ------------------ Configuration ------------------
PANEL_URL = "https://panel.businessidentity.llc"
API_KEY = "ptla_XjxL979xLjfkJ6mGhkukaNQu9qeCTg3YiE4uFrBOUpP"
REQUEST_TIMEOUT = (7, 7)  # in seconds
CONCURRENCY_LIMIT = 100      # Increase concurrency if resources allow
BATCH_SIZE = 500            # Process servers in batches of 100
PDF_RENDER_EXPORT_URL = "http://flask-outage-app:8080/upload"  # Use HTTP for internal service

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

# ------------------ Async Functions ------------------

async def async_check_ssl_handshake(hostname, timeout=7):
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
    except asyncio.TimeoutError:
        logging.error(f"SSL handshake timed out for {hostname}")
    except Exception as e:
        logging.error(f"SSL handshake failed for {hostname}: {e}")
    return False

async def async_get_with_retries(url, client, retries=2, delay=2):
    """Perform GET request with retries using httpx and exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            response = await client.get(url, follow_redirects=True)
            if 200 <= response.status_code < 400:
                return response.status_code
            else:
                logging.warning(f"Attempt {attempt}: {url} returned status {response.status_code}")
        except httpx.TimeoutException as te:
            logging.warning(f"Timeout on attempt {attempt} for {url}: {te}")
        except Exception as e:
            logging.warning(f"Attempt {attempt}: Error fetching {url}: {e}")
        await asyncio.sleep(delay * (2 ** (attempt - 1)))  # exponential backoff
    logging.error(f"All retries failed for {url}")
    return None

async def check_server_async(server, client, node_map):
    """Check server status asynchronously using httpx."""
    server_name = server["attributes"]["name"]
    external_id = server["attributes"].get("external_id")
    node_id = server["attributes"]["node"]
    node_name = node_map.get(node_id, f"Node {node_id}")

    if not external_id:
        return (server_name, node_name, False, False, False)

    server_url = f"https://{external_id}"
    mail_url = f"https://mail.{external_id}"

    web_status, mail_status = await asyncio.gather(
        async_get_with_retries(server_url, client),
        async_get_with_retries(mail_url, client)
    )
    ssl_status = await async_check_ssl_handshake(external_id)
    return (server_name, node_name, bool(web_status), bool(mail_status), ssl_status)

# ------------------ API Fetching ------------------

def get_all_nodes():
    """Fetch nodes from the API."""
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
    """Fetch servers from the API."""
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

# ------------------ Batch Processing ------------------

def batch_servers(servers, batch_size=BATCH_SIZE):
    """Yield successive batches from the servers list."""
    for i in range(0, len(servers), batch_size):
        yield servers[i:i + batch_size]

async def process_server_batch(servers, node_map, client):
    """Process a batch of servers asynchronously using httpx."""
    tasks = [check_server_async(server, client, node_map) for server in servers]
    batch_results = []
    for future in asyncio.as_completed(tasks):
        result = await future
        batch_results.append(result)
    return batch_results

# ------------------ Report Generation ------------------

def generate_report(results):
    """Generate an Excel report from the server check results."""
    now = datetime.datetime.now()
    filename = f"server_status_{now.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    df = pd.DataFrame(results, columns=["Server Name", "Node Name", "Web Status", "Mail Status", "SSL Valid"])
    df.to_excel(filename, index=False)
    logging.info(f"Excel report saved to {filename}")
    return filename

def analyze_excel_and_generate_pdf(excel_filename):
    """Convert the Excel report into a PDF report using ReportLab."""
    try:
        df = pd.read_excel(excel_filename)
    except Exception as e:
        logging.error(f"Error reading Excel file: {e}")
        return None

    # Verify required columns
    for col in ["Server Name", "Node Name", "Web Status", "Mail Status", "SSL Valid"]:
        if col not in df.columns:
            logging.error(f"Missing required column in report: {col}")
            return None

    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    now = datetime.datetime.now()
    pdf_filename = f"server_status_{now.strftime('%Y-%m-%d_%H-%M-%S')}.pdf"
    doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title and summary
    elements.append(Paragraph("Server Check Report", styles["Title"]))
    elements.append(Spacer(1, 24))
    summary_text = f"Report generated on {now.strftime('%Y-%m-%d %H:%M:%S')}"
    elements.append(Paragraph(summary_text, styles["Normal"]))
    elements.append(Spacer(1, 12))

    # Create table from DataFrame
    data = [df.columns.tolist()] + df.values.tolist()
    table = Table(data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(table)

    try:
        doc.build(elements)
        logging.info(f"PDF report generated: {pdf_filename}")
        return pdf_filename
    except Exception as e:
        logging.error(f"Error generating PDF: {e}")
        return None

def upload_report_to_pdf_service(pdf_filename):
    """Upload the PDF report to the PDF processing service."""
    try:
        with open(pdf_filename, 'rb') as f:
            files = {'file': (os.path.basename(pdf_filename), f, 'application/pdf')}
            response = requests.post(PDF_RENDER_EXPORT_URL, files=files)
        if response.status_code == 200:
            logging.info(f"PDF file {pdf_filename} successfully uploaded.")
        else:
            logging.error(f"Failed to upload PDF file {pdf_filename}. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error uploading PDF file: {e}")

# ------------------ Main Task Runner ------------------

async def run_server_check():
    """Main server check task with batch processing."""
    logging.info("Starting full server check...")
    node_map = get_all_nodes()
    servers = get_all_servers()
    total_servers = len(servers)
    logging.info(f"Total servers fetched: {total_servers}")
    all_results = []
    batch_num = 1

    # Create an httpx AsyncClient with limits
    limits = httpx.Limits(max_connections=CONCURRENCY_LIMIT, max_keepalive_connections=CONCURRENCY_LIMIT)
    async with httpx.AsyncClient(limits=limits, timeout=60) as client:
        for server_batch in batch_servers(servers, BATCH_SIZE):
            logging.info(f"Processing batch {batch_num} with {len(server_batch)} servers...")
            batch_results = await process_server_batch(server_batch, node_map, client)
            all_results.extend(batch_results)
            batch_num += 1
            gc.collect()

    excel_file = generate_report(all_results)
    if excel_file:
        pdf_file = analyze_excel_and_generate_pdf(excel_file)
        if pdf_file:
            upload_report_to_pdf_service(pdf_file)
    logging.info("Server check completed successfully.")

# ------------------ Scheduler Setup ------------------

scheduler = BlockingScheduler()

# Schedule the job to run daily at 7:30 AM PST (which is 15:30 UTC)
scheduler.add_job(lambda: asyncio.run(run_server_check()), 'cron', hour=15, minute=30, timezone='UTC')

# ------------------ Entry Point ------------------

if __name__ == "__main__":
    logging.info("Starting background worker; running server check once immediately...")
    asyncio.run(run_server_check())
    logging.info("Scheduling daily server check at 7:30 AM PST...")
    scheduler.start()
