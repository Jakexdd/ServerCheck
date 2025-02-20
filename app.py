#!/usr/bin/env python3
import os
import sys
import asyncio, aiohttp, requests, ssl, time, json, logging, datetime, pandas as pd, threading

# Monkey-patch Werkzeug's url_quote if it's missing
import werkzeug.urls
if not hasattr(werkzeug.urls, "url_quote"):
    from urllib.parse import quote
    werkzeug.urls.url_quote = quote

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

# Endpoint to which the PDF report should be sent
PDF_RENDER_EXPORT_URL = "https://flask-outage-app:8080/upload"  # MODIFY accordingly

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

# ------------------ Synchronous Functions for Server Check ------------------
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

# ------------------ Analyzer Functions ------------------
def analyze_excel_and_generate_pdf(excel_filename):
    """
    Loads the Excel file produced by the server check,
    computes statistics and generates charts,
    then builds a PDF report (using ReportLab) and returns its filename.
    """
    try:
        df = pd.read_excel(excel_filename)
    except Exception as e:
        logging.exception("Error reading Excel file: " + str(e))
        return None

    # Ensure required columns exist
    for col in ["Node Name", "Web Status", "Mail Status", "SSL"]:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            return None

    # Get the "Checked At" value if present
    checked_at = None
    if "Checked At" in df.columns:
        try:
            checked_at = pd.to_datetime(df["Checked At"].iloc[0])
        except Exception:
            checked_at = None

    # Overall stats
    total_servers = len(df)
    web_up = (df["Web Status"].str.upper() == "UP").sum()
    web_down = (df["Web Status"].str.upper() == "DOWN").sum()
    mail_up = (df["Mail Status"].str.upper() == "UP").sum()
    mail_down = (df["Mail Status"].str.upper() == "DOWN").sum()
    ssl_yes = (df["SSL"].str.upper() == "YES").sum()
    ssl_no = (df["SSL"].str.upper() == "NO").sum()

    # Group by "Node Name" for breakdown
    node_group = df.groupby("Node Name").agg(
        Web_Up=('Web Status', lambda x: (x.str.upper() == "UP").sum()),
        Web_Down=('Web Status', lambda x: (x.str.upper() == "DOWN").sum()),
        Mail_Up=('Mail Status', lambda x: (x.str.upper() == "UP").sum()),
        Mail_Down=('Mail Status', lambda x: (x.str.upper() == "DOWN").sum()),
        SSL_Yes=('SSL', lambda x: (x.str.upper() == "YES").sum()),
        SSL_No=('SSL', lambda x: (x.str.upper() == "NO").sum())
    ).reset_index()
    node_group.columns = [col.replace("_", " ") for col in node_group.columns]

    if not node_group.empty:
        most_problematic_node = node_group.loc[node_group['Web Down'].idxmax(), "Node Name"]
        most_stable_node = node_group.loc[node_group['Web Up'].idxmax(), "Node Name"]
    else:
        most_problematic_node = "N/A"
        most_stable_node = "N/A"

    summary_text = (
        f"Total Servers: {total_servers}\n"
        f"Web Up: {web_up}\n"
        f"Web Down: {web_down}\n"
        f"Mail Up: {mail_up}\n"
        f"Mail Down: {mail_down}\n"
        f"SSL Enabled: {ssl_yes}\n"
        f"SSL Disabled: {ssl_no}\n"
        f"Most Problematic Node: {most_problematic_node}\n"
        f"Most Stable Node: {most_stable_node}"
    )

    # Compute extra percentages
    web_up_pct = (web_up / (web_up + web_down) * 100) if (web_up + web_down) > 0 else 0
    web_down_pct = (web_down / (web_up + web_down) * 100) if (web_up + web_down) > 0 else 0
    mail_up_pct = (mail_up / (mail_up + mail_down) * 100) if (mail_up + mail_down) > 0 else 0
    mail_down_pct = (mail_down / (mail_up + mail_down) * 100) if (mail_up + mail_down) > 0 else 0
    ssl_yes_pct = (ssl_yes / (ssl_yes + ssl_no) * 100) if (ssl_yes + ssl_no) > 0 else 0
    ssl_no_pct = (ssl_no / (ssl_yes + ssl_no) * 100) if (ssl_yes + ssl_no) > 0 else 0

    extra_stats_text = (
        f"Web Server Stats:\nUp: {web_up} ({web_up_pct:.1f}%)\nDown: {web_down} ({web_down_pct:.1f}%)\n\n"
        f"Mail Server Stats:\nUp: {mail_up} ({mail_up_pct:.1f}%)\nDown: {mail_down} ({mail_down_pct:.1f}%)\n\n"
        f"SSL Server Stats:\nEnabled: {ssl_yes} ({ssl_yes_pct:.1f}%)\nDisabled: {ssl_no} ({ssl_no_pct:.1f}%)"
    )

    # Generate charts in headless mode
    import matplotlib.pyplot as plt
    plt.style.use('dark_background')

    # Bar Chart
    plt.figure(figsize=(14, 7))
    index = range(len(node_group))
    bar_width = 0.35
    bars_web = plt.bar([i - bar_width/2 for i in index], node_group["Web Down"], bar_width, label='Web Down', color='#F44336')
    bars_mail = plt.bar([i + bar_width/2 for i in index], node_group["Mail Down"], bar_width, label='Mail Down', color='#FF9800')
    for bar in bars_web:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{int(bar.get_height())}',
                 ha='center', va='bottom', fontsize=10, color='white')
    for bar in bars_mail:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{int(bar.get_height())}',
                 ha='center', va='bottom', fontsize=10, color='white')
    plt.xlabel("Node Name", fontsize=12, color='white')
    plt.ylabel("Count Down", fontsize=12, color='white')
    plt.title("Number of Web and Mail Servers Down per Node", fontsize=14, color='white')
    plt.xticks(index, node_group["Node Name"], rotation=45, fontsize=10, color='white')
    plt.legend()
    plt.tight_layout()
    bar_chart_path = "node_status.png"
    plt.savefig(bar_chart_path)
    plt.close()

    # Pie Charts
    fig, axs = plt.subplots(1, 3, figsize=(21, 7))
    plt.subplots_adjust(wspace=0.4, left=0.1, right=0.9, top=0.95, bottom=0.1)
    # Web status
    axs[0].pie([web_up, web_down], labels=['Up', 'Down'], autopct='%1.1f%%', startangle=90,
               colors=['#8BC34A', '#F44336'], shadow=True)
    axs[0].set_title("Web Server Status", fontsize=12, pad=20, color='white')
    # Mail status
    axs[1].pie([mail_up, mail_down], labels=['Up', 'Down'], autopct='%1.1f%%', startangle=90,
               colors=['#8BC34A', '#F44336'], shadow=True)
    axs[1].set_title("Mail Server Status", fontsize=12, pad=20, color='white')
    # SSL status
    axs[2].pie([ssl_yes, ssl_no], labels=['Enabled', 'Disabled'], autopct='%1.1f%%', startangle=90,
               colors=['#8BC34A', '#F44336'], shadow=True)
    axs[2].set_title("SSL Server Status", fontsize=12, pad=20, color='white')
    pie_chart_path = "status_pie.png"
    plt.savefig(pie_chart_path)
    plt.close()

    # Build PDF report using ReportLab
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image as RLImage, LongTable, Table, TableStyle, KeepTogether
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors

    try:
        dt = datetime.datetime.now()
        if checked_at:
            pdf_filename = checked_at.strftime("%B_%d_%Y_%I-%M-%S_%p.pdf")
        else:
            pdf_filename = f"server_stats_{dt.strftime('%Y%m%d_%H%M%S')}.pdf"
        doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
        styles = getSampleStyleSheet()
        flowables = []
        flowables.append(Paragraph("Stat Compiler v2 Report", styles["Title"]))
        flowables.append(Spacer(1, 24))
        if checked_at:
            date_str = checked_at.strftime("%B %d, %Y %I:%M %p")
            flowables.append(Paragraph(f"<b>Checked At:</b> {date_str}", styles["Heading2"]))
            flowables.append(Spacer(1, 12))
        summary_html = summary_text.replace('\n', '<br/>')
        summary_para = Paragraph(f"<b>Overall Summary:</b><br/>{summary_html}", styles["BodyText"])
        extra_html = extra_stats_text.replace('\n', '<br/>')
        additional_para = Paragraph(f"<b>Additional Info:</b><br/>{extra_html}", styles["BodyText"])
        summary_table_data = [[summary_para, additional_para]]
        summary_table = Table(summary_table_data, colWidths=[doc.width/2.0, doc.width/2.0])
        summary_table.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 0.25, colors.black),
        ]))
        flowables.append(summary_table)
        flowables.append(Spacer(1, 12))
        # Convert node_group to table data
        data = [node_group.columns.tolist()] + node_group.values.tolist()
        node_table = LongTable(data, repeatRows=1)
        node_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        flowables.append(node_table)
        flowables.append(Spacer(1, 24))
        if os.path.exists(bar_chart_path):
            flowables.append(Paragraph("<b>Bar Chart: Number of Web and Mail Servers Down per Node</b>", styles["BodyText"]))
            flowables.append(Spacer(1, 12))
            flowables.append(RLImage(bar_chart_path, width=400, height=300))
            flowables.append(Spacer(1, 24))
        if os.path.exists(pie_chart_path):
            pie_chart_flowable = KeepTogether([
                Paragraph("<b>Pie Charts: Server Status</b>", styles["BodyText"]),
                Spacer(1, 12),
                RLImage(pie_chart_path, width=400, height=133),
                Spacer(1, 24)
            ])
            flowables.append(pie_chart_flowable)
        doc.build(flowables)
    except Exception as e:
        logging.exception("Error generating PDF: " + str(e))
        return None

    return pdf_filename

def export_pdf_file_to_remote(pdf_filename):
    try:
        with open(pdf_filename, 'rb') as f:
            files = {'file': (pdf_filename, f, 'application/pdf')}
            response = requests.post(PDF_RENDER_EXPORT_URL, files=files)
            if response.status_code == 200:
                logging.info(f"PDF file {pdf_filename} successfully uploaded to remote server.")
            else:
                logging.error(f"Failed to upload PDF file {pdf_filename} to remote server. Status: {response.status_code}")
    except Exception as e:
        logging.exception("Error exporting PDF file to remote server.")

# ------------------ Main Server Check Routine ------------------
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
    excel_filename = export_results_to_excel(server_status)
    if excel_filename:
        # Analyze the Excel file to generate a PDF report and upload that PDF
        pdf_filename = analyze_excel_and_generate_pdf(excel_filename)
        if pdf_filename:
            export_pdf_file_to_remote(pdf_filename)
    return server_status, excel_filename

# ------------------ Flask Application ------------------
app = Flask(__name__)
check_status = {
    "running": False,
    "result": None,
    "excel_file": None,
    "message": ""
}

@app.route("/")
def index():
    return "Server Check Flask App is running. Visit /status to view the current status."

@app.route("/status")
def status():
    return jsonify(check_status)

def background_check():
    check_status["running"] = True
    check_status["message"] = "Server check in progress..."
    try:
        result, excel_file = run_server_check()
        check_status["result"] = result
        check_status["excel_file"] = excel_file
        check_status["message"] = "Server check completed."
    except Exception as e:
        check_status["message"] = f"Error during server check: {e}"
        logging.exception("Error in background server check")
    finally:
        check_status["running"] = False

if __name__ == "__main__":
    # Start the background check in a separate thread before starting the Flask server.
    thread = threading.Thread(target=background_check)
    thread.start()
    app.run(host="0.0.0.0", port=5000)
