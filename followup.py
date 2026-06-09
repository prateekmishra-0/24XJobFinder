import os
import time
import datetime
import requests
import pymongo
from dotenv import load_dotenv

# Load local .env (does nothing in GitHub Actions — secrets are injected as env vars)
load_dotenv()

# ==========================================
# SECURE CONFIGURATION (Environment Variables)
# Must match the 5 GitHub Secrets defined in the architecture exactly.
# ==========================================
MONGO_URI           = os.environ.get("MONGO_URI")
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID")

if not all([MONGO_URI, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    raise ValueError("CRITICAL: Missing environment variables. followup.py aborted.")

# ==========================================
# DATABASE CONNECTION
# Mirrors hunter.py exactly — same client, same db name, same collection names.
# ==========================================
mongo_client    = pymongo.MongoClient(MONGO_URI)
db              = mongo_client["job_hunter"]
jobs_col        = db["jobs"]
meta_col        = db["followup_meta"]   # Stores Telegram polling offset between runs

BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# ==========================================
# HOW THIS SCRIPT WORKS (read before editing)
#
# This script is designed to run as a SHORT-LIVED GitHub Actions job
# triggered every 7 minutes by followup.yml. It is NOT an infinite loop.
# It starts, does its work, and exits. GitHub Actions starts it again 7 min later.
#
# Two responsibilities on every run:
#
#   PART 1 — CALLBACK PROCESSING
#   Read the last processed Telegram update ID from MongoDB (followup_meta collection).
#   Pass it as `offset` to Telegram getUpdates so we only receive NEW button presses.
#   For every new callback: DELETE the job document from the jobs collection (the hash
#   already lives in seen_hashes — hunter.py wrote it there when the job was found),
#   answer the callback to stop the loading clock, remove the inline buttons.
#   Save the new max update ID back to MongoDB so the next run starts where we left off.
#
#   PART 2 — 5-DAY REMINDER SCAN
#   Query the jobs collection for any job where action == null AND saved_at is older
#   than 5 days AND reminder_sent != true.
#   For each such job: send a reminder message to Telegram with the same buttons.
#   Write reminder_sent: true to the job document so it is never re-sent.
# ==========================================


# ==========================================
# PART 1 HELPERS — TELEGRAM CALLBACK PROCESSING
# ==========================================

def get_offset():
    """Read the last processed Telegram update_id from MongoDB.
    Returns None if this is the very first run (no offset stored yet)."""
    doc = meta_col.find_one({"_id": "telegram_offset"})
    if doc:
        return doc.get("offset")
    return None


def save_offset(offset):
    """Persist the last processed update_id to MongoDB so the next run
    picks up exactly where this run left off. Prevents re-processing old clicks."""
    meta_col.update_one(
        {"_id": "telegram_offset"},
        {"$set": {"offset": offset}},
        upsert=True
    )


def fetch_updates(offset):
    """Call Telegram getUpdates with the stored offset.
    timeout=0 makes this a non-blocking poll — we do not wait for new updates.
    We only want whatever has already arrived since the last run."""
    params = {"timeout": 0}
    if offset is not None:
        params["offset"] = offset
    try:
        resp = requests.get(f"{BASE_URL}/getUpdates", params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("result", [])
        print(f"⚠️  getUpdates returned status {resp.status_code}")
    except Exception as e:
        print(f"⚠️  getUpdates error: {e}")
    return []


def answer_callback(callback_id, text):
    """Acknowledge the button press to Telegram.
    This stops the loading spinner on the button in the user's Telegram client."""
    try:
        requests.post(
            f"{BASE_URL}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=10
        )
    except Exception as e:
        print(f"⚠️  answerCallbackQuery error: {e}")


def remove_buttons(chat_id, message_id):
    """Edit the original job notification message to remove the inline keyboard.
    This gives the user clear visual confirmation that their action was recorded."""
    try:
        requests.post(
            f"{BASE_URL}/editMessageReplyMarkup",
            json={
                "chat_id":      chat_id,
                "message_id":   message_id,
                "reply_markup": {"inline_keyboard": []}  # Empty list removes all buttons
            },
            timeout=10
        )
    except Exception as e:
        print(f"⚠️  editMessageReplyMarkup error: {e}")


def process_callbacks():
    """
    PART 1 — Main callback processing loop.

    Fetches all Telegram updates since the last stored offset, processes any
    button presses, and saves the new offset back to MongoDB.

    Architecture spec (system overview diagram):
      On Applied → delete job document from jobs collection
                   hash already in seen_hashes — no action needed there
      On Skip    → same

    The hash stays in seen_hashes because hunter.py wrote it there at discovery
    time. The 14-day TTL on the jobs document is irrelevant — we delete it
    immediately on button press to keep the collection clean.
    """
    print("📥 PART 1: Fetching Telegram callbacks...")
    offset = get_offset()
    updates = fetch_updates(offset)

    if not updates:
        print("✅  No new callbacks since last run.")
        return

    new_offset = offset
    processed  = 0

    for update in updates:
        update_id = update.get("update_id")

        # Track the highest update_id seen — this becomes the new offset
        if new_offset is None or update_id >= new_offset:
            new_offset = update_id + 1

        # Only process callback_query events (button presses)
        if "callback_query" not in update:
            continue

        cb      = update["callback_query"]
        cb_id   = cb["id"]
        data    = cb.get("data", "")
        message = cb.get("message", {})
        chat_id    = message.get("chat", {}).get("id")
        message_id = message.get("message_id")

        # Validate callback data format: must be "action:hash"
        if ":" not in data:
            print(f"⚠️  Unexpected callback data format: '{data}' — skipping.")
            continue

        action, job_hash = data.split(":", 1)

        if action == "applied":
            # Architecture: On Applied → delete job from jobs collection
            # Hash is already in seen_hashes — hunter.py wrote it at discovery time.
            result = jobs_col.delete_one({"_id": job_hash})
            if result.deleted_count > 0:
                answer_callback(cb_id, "✅ Marked as Applied!")
                remove_buttons(chat_id, message_id)
                print(f"✅ APPLIED — job deleted from jobs collection. Hash: {job_hash}")
                processed += 1
            else:
                # Job document already gone (TTL expired after 14 days) — still ack the click
                answer_callback(cb_id, "✅ Recorded (job data has expired)")
                remove_buttons(chat_id, message_id)
                print(f"⚠️  Job hash {job_hash} not found in DB (likely TTL-expired) — ack'd anyway.")

        elif action == "skip":
            # Architecture: On Skip → delete job from jobs collection (same as Applied)
            result = jobs_col.delete_one({"_id": job_hash})
            if result.deleted_count > 0:
                answer_callback(cb_id, "❌ Job skipped.")
                remove_buttons(chat_id, message_id)
                print(f"❌ SKIPPED — job deleted from jobs collection. Hash: {job_hash}")
                processed += 1
            else:
                answer_callback(cb_id, "❌ Recorded (job data has expired)")
                remove_buttons(chat_id, message_id)
                print(f"⚠️  Job hash {job_hash} not found in DB (likely TTL-expired) — ack'd anyway.")

        else:
            print(f"⚠️  Unknown action '{action}' in callback data — skipping.")

    # Always persist the new offset, even if no callbacks were processed.
    # This advances the offset past non-callback update types (message edits, etc.)
    if new_offset is not None and new_offset != offset:
        save_offset(new_offset)
        print(f"💾  Offset saved: {new_offset}")

    print(f"✅  Processed {processed} callback(s).")


# ==========================================
# PART 2 HELPERS — 5-DAY REMINDER SCAN
# ==========================================

def send_reminder(job):
    """
    Build and send the 5-day reminder message per the exact format in the architecture:

        ⏰ REMINDER — You haven't acted on this yet

        💼 [Job Title] @ [Company]
        📍 [Location] | Tier [X]

        Applied for a referral? If not, apply directly now.
        Original match from: DD Mon YYYY

        🔗 Apply Here

        [✅ Applied]  [❌ Skip]

    The same inline keyboard is attached so the candidate can act from the reminder.
    """
    job_hash   = job["_id"]
    title      = job.get("job_title", "Unknown Role")
    company    = job.get("company",   "Unknown Company")
    location   = job.get("location",  "")
    tier       = job.get("tier",      "?")
    url        = job.get("url",       "")
    saved_at   = job.get("saved_at")

    # Format the original match date
    if saved_at and isinstance(saved_at, datetime.datetime):
        date_str = saved_at.strftime("%d %b %Y")
    else:
        date_str = "unknown date"

    location_tier_line = f"📍 {location} | Tier {tier}" if location else f"Tier {tier}"

    message = (
        f"⏰ *REMINDER — You haven't acted on this yet*\n\n"
        f"💼 {title} @ {company}\n"
        f"{location_tier_line}\n\n"
        f"Applied for a referral? If not, apply directly now.\n"
        f"Original match from: {date_str}\n\n"
        f"[Apply Here]({url})"
    )

    payload = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "✅ Applied", "callback_data": f"applied:{job_hash}"},
                {"text": "❌ Skip",    "callback_data": f"skip:{job_hash}"},
            ]]
        }
    }

    try:
        resp = requests.post(f"{BASE_URL}/sendMessage", json=payload, timeout=10)
        if resp.status_code == 200:
            print(f"⏰  Reminder sent — {title} @ {company}")
            return True
        print(f"⚠️  Telegram returned {resp.status_code} on reminder for hash {job_hash}")
    except Exception as e:
        print(f"❌  Failed to send reminder for hash {job_hash}: {e}")

    return False


def scan_for_reminders():
    """
    PART 2 — 5-day reminder scan.

    Queries the jobs collection for documents where:
      - action is null (candidate has not pressed Applied or Skip)
      - saved_at is older than 5 days
      - reminder_sent is not true (reminder has not already been sent)

    For each match: send reminder, then write reminder_sent: true so it is
    never triggered again regardless of how many times followup.py runs.

    Architecture spec: "Reminder is sent only once."
    """
    print("🔍 PART 2: Scanning for unactioned jobs older than 5 days...")

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=5)

    # Find all jobs that need a reminder
    # action == null: no button pressed
    # saved_at < cutoff: posted more than 5 days ago
    # reminder_sent != true: not yet reminded (covers both missing field and false)
    jobs_needing_reminder = list(jobs_col.find({
        "action":        None,
        "saved_at":      {"$lt": cutoff},
        "reminder_sent": {"$ne": True}
    }))

    if not jobs_needing_reminder:
        print("✅  No unactioned jobs past 5-day threshold.")
        return

    print(f"📋  Found {len(jobs_needing_reminder)} job(s) requiring a reminder.")

    for job in jobs_needing_reminder:
        sent = send_reminder(job)

        if sent:
            # Mark reminder_sent: true immediately so a re-run 7 minutes later
            # does not send the same reminder again. This is the "only once" guarantee.
            jobs_col.update_one(
                {"_id": job["_id"]},
                {"$set": {"reminder_sent": True}}
            )
            # Small pause between reminders if multiple jobs need sending
            time.sleep(1)

    print(f"✅  Reminder scan complete.")


# ==========================================
# MAIN — Short-lived entry point for GitHub Actions
#
# This function runs once and exits. GitHub Actions invokes it every 7 minutes.
# There is no while True loop here — that design is incompatible with GitHub Actions.
# ==========================================
def run():
    print("🚀 followup.py starting...")

    process_callbacks()   # Part 1: handle button presses
    scan_for_reminders()  # Part 2: send 5-day nudges

    print("🏁 followup.py complete.")


if __name__ == "__main__":
    run()
