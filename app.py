import asyncio
import logging
import time
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz

# ------------------ Logging Setup ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ------------------ Flask App Setup ------------------
app = Flask(__name__)

# Status check dictionary to monitor background execution
check_status = {
    "running": False,
    "last_run": None,
    "result": None,
    "message": ""
}

# ------------------ Background Task Functions ------------------
def run_scheduled_tasks():
    """Main server check execution"""
    try:
        logging.info("Scheduled server check started...")
        print("Scheduled server check started...")  # Print statement for Render logs
        
        check_status["running"] = True
        check_status["last_run"] = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Simulate long-running task
        time.sleep(10)  # Placeholder for server-check logic

        check_status["result"] = "Server check completed successfully."
        check_status["message"] = "Server check finished."
        
        logging.info("Server check completed successfully.")
        print("Server check completed successfully.")
    except Exception as e:
        logging.error(f"Error during server check: {e}")
        print(f"Error during server check: {e}")
        check_status["message"] = f"Error: {str(e)}"
    finally:
        check_status["running"] = False


# ------------------ Scheduler Setup ------------------
scheduler = BackgroundScheduler(timezone="UTC")

# Schedule to run every day at 7:30 AM PST (which is 15:30 UTC)
scheduler.add_job(run_scheduled_tasks, 'cron', hour=15, minute=30)
scheduler.start()

# ------------------ Flask Routes ------------------
@app.route("/")
def index():
    """Basic health check"""
    return jsonify({
        "message": "Server Check Worker is running.",
        "status": check_status
    })

@app.route("/trigger")
def trigger_check():
    """Manually trigger the server check"""
    if not check_status["running"]:
        logging.info("Manual trigger received; starting server check.")
        print("Manual trigger received; starting server check.")
        run_scheduled_tasks()
        return jsonify({"message": "Server check manually triggered."})
    else:
        return jsonify({"message": "Server check is already running."})

@app.route("/status")
def status():
    """Check the status of the latest server check"""
    return jsonify(check_status)

# ------------------ App Runner ------------------
if __name__ == "__main__":
    logging.info("Server Check Worker has started and is ready for scheduled tasks.")
    print("Server Check Worker has started and is ready for scheduled tasks.")
    app.run(host="0.0.0.0", port=5000)
