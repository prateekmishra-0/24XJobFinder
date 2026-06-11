import os
import re
import time
import hashlib
import json
import datetime
import requests
import pymongo
from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import APIError

# Load local environment variables from .env file (Does nothing in GitHub Actions)
load_dotenv()

# ==========================================
# SECURE CONFIGURATION (Environment Variables)
# ==========================================
MONGO_URI = os.environ.get("MONGO_URI")
TINYFISH_API_KEY = os.environ.get("TINYFISH_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Load Gemini keys as a JSON array
_raw_keys = os.environ.get("GEMINI_API_KEYS", "[]")
try:
    GEMINI_KEY_POOL = json.loads(_raw_keys)
except json.JSONDecodeError:
    raise ValueError("CRITICAL: GEMINI_API_KEYS must be a valid JSON array of strings.")

if not all([MONGO_URI, TINYFISH_API_KEY]) or not GEMINI_KEY_POOL:
    raise ValueError("CRITICAL: Missing essential environment variables. Pipeline aborted.")

# Initialize client APIs
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["job_hunter"]
seen_hashes_col = db["seen_hashes"]
jobs_col = db["jobs"]
meta_col = db["followup_meta"]

# ==========================================
# API KEY ROTATION STATE
# ==========================================
_current_key_index = None
_ai_client = None
_keys_tried_this_rotation = set()  # Tracks which keys have 429'd in the current rotation cycle

def get_active_key_index():
    doc = meta_col.find_one({"_id": "gemini_key_index"})
    if doc:
        return doc.get("index", 0) % len(GEMINI_KEY_POOL)
    return 0

def save_active_key_index(index):
    meta_col.update_one(
        {"_id": "gemini_key_index"},
        {"$set": {"index": index % len(GEMINI_KEY_POOL)}},
        upsert=True
    )

def get_next_key_index(current_index):
    return (current_index + 1) % len(GEMINI_KEY_POOL)

def get_ai_client():
    global _current_key_index, _ai_client, _keys_tried_this_rotation
    if _current_key_index is None:
        _current_key_index = get_active_key_index()
        _keys_tried_this_rotation = {_current_key_index}  # Seed with starting key
        _ai_client = genai.Client(api_key=GEMINI_KEY_POOL[_current_key_index])
        print(f"🔑 Gemini key #{_current_key_index} active.")
    return _ai_client

def rotate_key():
    """Called when 429 hits. Switches to next key and persists it.
    Returns False when we've tried every key in the pool and all are exhausted."""
    global _current_key_index, _ai_client, _keys_tried_this_rotation

    next_index = get_next_key_index(_current_key_index)

    # Only one key in pool — nothing to rotate to
    if next_index == _current_key_index:
        return False

    # We've already tried this key and it 429'd — full pool exhausted
    if next_index in _keys_tried_this_rotation:
        return False

    _keys_tried_this_rotation.add(next_index)
    print(f"🔄 Rotating Gemini key: #{_current_key_index} → #{next_index}")
    _current_key_index = next_index
    _ai_client = genai.Client(api_key=GEMINI_KEY_POOL[_current_key_index])
    save_active_key_index(_current_key_index)
    return True


# ==========================================
# QUERY BOOKMARK — single integer in MongoDB
# ==========================================
def get_query_bookmark():
    """Returns the query index to start from. 0 = start fresh."""
    doc = meta_col.find_one({"_id": "query_bookmark"})
    if doc:
        return doc.get("index", 0)
    return 0

def save_query_bookmark(index):
    """Persist current query index so a hard-killed run can resume."""
    meta_col.update_one(
        {"_id": "query_bookmark"},
        {"$set": {"index": index}},
        upsert=True
    )

def reset_query_bookmark():
    """Called on clean finish or quota exhaustion — next run starts from scratch."""
    save_query_bookmark(0)


# ==========================================
# QUERY BANK — 40 queries across all sources
# ==========================================
QUERY_BANK = [
    # ------------------------------------------
    # [A] GREENHOUSE — Global ATS (Primary)
    # Location terms added to all queries that lacked them.
    # ------------------------------------------
    'site:greenhouse.io "Java" "Spring Boot" "Backend" "India" OR "Remote" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager" -"Director"',
    'site:job-boards.greenhouse.io "Backend Engineer" "Java" "India" OR "Remote" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager"',
    'site:greenhouse.io "Java Developer" "India" OR "Remote" "fresher" OR "entry level" OR "0-1" -"Senior" -"Lead" -"Manager"',
    'site:greenhouse.io "Java" "Spring Boot" "Bangalore" OR "Bengaluru" OR "Hyderabad" OR "Pune" OR "Mumbai" OR "Noida" OR "Gurugram" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:greenhouse.io "Software Engineer" "Java" "Backend" "India" OR "Remote" "2026" OR "fresher" -"Senior" -"Staff" -"Manager"',

    # ------------------------------------------
    # [B] LEVER — Product startups globally
    # ------------------------------------------
    'site:lever.co "Java" "Spring Boot" "Backend" "India" OR "Remote" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager" -"Director"',
    'site:lever.co "Java Developer" "India" OR "Remote" "fresher" OR "entry level" OR "0-2 years" -"Senior" -"Lead" -"Manager"',
    'site:lever.co "Backend Engineer" "Java" "Bangalore" OR "Bengaluru" OR "Hyderabad" OR "Remote" -"Senior" -"Manager" -"Director"',

    # ------------------------------------------
    # [C] ASHBY — High-growth startups (newer ATS)
    # ------------------------------------------
    'site:jobs.ashbyhq.com "Java" "Spring Boot" "Backend" "India" OR "Remote" -"Senior" -"Lead" -"Staff" -"Manager" -"Director"',
    'site:jobs.ashbyhq.com "Backend Engineer" "Java" "India" OR "Remote" "entry level" OR "fresher" OR "0-1" -"Senior" -"Manager"',

    # ------------------------------------------
    # [D] WORKDAY — Large enterprises and MNCs
    # ------------------------------------------
    'site:myworkdayjobs.com "Java" "Spring Boot" "India" OR "Bangalore" OR "Bengaluru" OR "Hyderabad" OR "Pune" OR "Mumbai" OR "Noida" OR "Gurugram" OR "Gurgaon" OR "Chennai" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:myworkdayjobs.com "Java" "Backend" "India" OR "Bangalore" OR "Bengaluru" OR "Hyderabad" OR "Pune" -"Senior" -"Lead" -"Manager" -"Director"',
    '"myworkdayjobs.com" "Java" "Spring Boot" "India" "fresher" OR "entry level" -"Senior" -"Lead"',
    '"myworkdayjobs.com" "Software Engineer" "Java" "India" 2026 -"Senior" -"Lead" -"Manager"',

    # ------------------------------------------
    # [E] SKILL-SPECIFIC ROTATION
    # ------------------------------------------
    'site:greenhouse.io "Spring Data REST" OR "Spring Security" "Java" "India" OR "Remote" -"Senior" -"Lead" -"Manager"',
    'site:lever.co "Spring Security" "Java" "Backend" "India" OR "Remote" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:greenhouse.io "microservices" "Java" "India" OR "Remote" "entry level" OR "fresher" OR "0-1" -"Senior" -"Lead" -"Manager"',
    'site:naukri.com "Spring Security" "Java" "fresher" OR "0-1 years" -"Senior" -"Lead"',

    # ------------------------------------------
    # [F] NAUKRI — Indian IT and domestic market
    # ------------------------------------------
    'site:naukri.com "Java" "Spring Boot" "fresher" 2026 -"Senior" -"Lead"',
    'site:naukri.com "Backend Engineer" "Java" "0-1 years" OR "0-2 years" -"Senior" -"Lead"',
    'site:naukri.com "Java Developer" "Spring Boot" "Bangalore" OR "Hyderabad" OR "Pune" -"Senior"',
    'site:naukri.com "Software Engineer" "Java" "Spring Boot" "fresher" -"Senior" -"Lead"',

    # ------------------------------------------
    # [G] INSTAHYRE / WELLFOUND / INTERNSHALA
    # ------------------------------------------
    'site:instahyre.com "Java Backend" "fresher" OR "entry level" -"Senior" -"Lead"',
    'site:instahyre.com "Java" "Spring Boot" "0-1" OR "0-2" -"Senior"',
    'site:wellfound.com "Backend Engineer" "Java" "India" OR "Remote" "entry level" -"Senior" -"Lead" -"Principal"',
    'site:wellfound.com "Java Developer" "Spring Boot" "India" -"Senior" -"Staff"',
    'site:internshala.com "Java" "Spring Boot" "backend" -"Senior"',

    # ------------------------------------------
    # [H] INDIAN PRODUCT COMPANIES — via aggregators
    # ------------------------------------------
    'site:naukri.com "Razorpay" OR "PhonePe" OR "CRED" "Java" "0-1 years" OR "0-2 years" OR "fresher" -"Senior"',
    'site:naukri.com "Flipkart" OR "Swiggy" OR "Zomato" "Java" "Backend" "fresher" OR "0-1" -"Senior" -"Lead"',
    'site:naukri.com "Paytm" OR "MakeMyTrip" OR "Meesho" "Java" "Spring Boot" "fresher" OR "0-1 years" -"Senior"',
    'site:greenhouse.io "Razorpay" OR "PhonePe" OR "Swiggy" "Java" "Backend" -"Senior" -"Lead" -"Manager"',
    'site:lever.co "Flipkart" OR "Zomato" OR "CRED" OR "Meesho" "Java" "Backend" -"Senior" -"Lead"',

    # ------------------------------------------
    # [I] MNC INDIA ARMS — Amazon, Microsoft, Goldman, Morgan Stanley
    # ------------------------------------------
    'site:amazon.jobs "Java" "Software Engineer" "India" "entry level" OR "fresher" -"Senior" -"Principal" -"Manager"',
    'site:careers.walmart.com "Java" "Backend" "India" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:careers.google.com "Java" "Software Engineer" "India" -"Senior" -"Staff" -"Principal" -"Manager"',
    'site:careers.microsoft.com "Java" "Software Engineer" "India" -"Senior" -"Principal" -"Manager" -"Director"',
    'site:goldmansachs.com/careers "Java" "Software Engineer" "India" -"Senior" -"Vice President" -"Managing Director"',
    'site:morganstanley.com/people/careers "Java" "Software Engineer" "India" -"Senior" -"Manager" -"Director"',

    # ------------------------------------------
    # [J] INFOSYS / WIPRO — Indian IT giants on their own domains
    # ------------------------------------------
    'site:careers.infosys.com "Java" "Spring Boot" "fresher" 2026',
    'site:careers.wipro.com "Java" "Spring Boot" "fresher" 2026',
]


# ==========================================
# DOMAIN ALLOWLIST — the primary URL gate
#
# A URL is only processed if its domain
# matches one of these known job platforms.
# Anything else (Facebook, Instagram, Telegram,
# blogs, news sites, etc.) is dropped immediately
# before any HTTP call or fetch is made.
# ==========================================
ALLOWED_DOMAINS = [
    # ATS platforms
    "greenhouse.io",
    "job-boards.greenhouse.io",
    "lever.co",
    "jobs.ashbyhq.com",
    "myworkdayjobs.com",
    # Indian job boards
    "naukri.com",
    "instahyre.com",
    "wellfound.com",
    "internshala.com",
    # Company career pages
    "amazon.jobs",
    "careers.walmart.com",
    "careers.google.com",
    "careers.microsoft.com",
    "goldmansachs.com",
    "morganstanley.com",
    "careers.infosys.com",
    "careers.wipro.com",
    "capco.com",
    # LinkedIn — only the /jobs/view/ path is a real job page
    "linkedin.com",
]

# Domains that are never job pages, no matter what the search returns.
# Checked BEFORE the allowlist so accidental allowlist additions can't
# let through social / blog content.
BLOCKED_DOMAINS = [
    "facebook.com",
    "instagram.com",
    "t.me",
    "telegram.me",
    "twitter.com",
    "x.com",
    "youtube.com",
    "medium.com",
    "substack.com",
    "blogspot.com",
    "wordpress.com",
    "reddit.com",
    "quora.com",
    "whatsapp.com",
    "threads.net",
    "tiktok.com",
]

def url_is_from_allowed_domain(url):
    """
    Returns True only when the URL's domain is on the allowlist
    AND not on the blocklist.

    Special case for LinkedIn: only linkedin.com/jobs/view/ URLs are
    real job pages. linkedin.com/posts/, /feed/, /in/ etc. are not.

    Naukri has its own structural check — only /job-listings-* URLs
    with a 6+ digit job ID are real listings.
    """
    url_lower = url.lower()

    # Step 1 — hard block social / blog domains first
    for blocked in BLOCKED_DOMAINS:
        if blocked in url_lower:
            return False

    # Step 2 — LinkedIn special case: must be a /jobs/view/ URL
    if "linkedin.com" in url_lower:
        return "/jobs/view/" in url_lower

    # Step 3 — Naukri special case: must match the job listing URL pattern
    if "naukri.com" in url_lower:
        return bool(re.search(r'/job-listings-.+\d{6,}', url_lower))

    # Step 4 — all other domains: must appear in the allowlist
    for allowed in ALLOWED_DOMAINS:
        if allowed in url_lower:
            return True

    return False


# ==========================================
# LOCATION FILTER — allowlist + blocklist
# ==========================================
LOCATION_ALLOWLIST = [
    "india", "remote", "bangalore", "bengaluru", "pune", "hyderabad",
    "mumbai", "noida", "gurugram", "gurgaon", "chennai", "delhi",
    "kolkata", "cochin", "kochi", "chandigarh",
]

LOCATION_BLOCKLIST = [
    "us only", "united states", "usa", "uk only", "united kingdom",
    "europe", "dubai", "singapore", "germany", "canada", "australia",
]

def location_is_acceptable(location_str):
    if not location_str:
        return True
    loc = location_str.lower()
    for term in LOCATION_BLOCKLIST:
        if term in loc:
            return False
    for term in LOCATION_ALLOWLIST:
        if term in loc:
            return True
    return False


# Tier display labels for Telegram notifications
TIER_LABEL = {
    "A": "🔥 STRONG MATCH — Apply Today",
    "B": "✅ GOOD MATCH — Worth Applying",
    "C": "👀 BORDERLINE — Your Call"
}

# ==========================================
# DYNAMIC CANDIDATE DATA LOADER
# ==========================================
def load_candidate_data():
    profile_text = "No profile data loaded."
    resume_text = "No resume data loaded."

    try:
        if os.path.exists("candidate_profile.json"):
            with open("candidate_profile.json", "r") as f:
                profile_text = json.dumps(json.load(f), indent=2)
        else:
            print("⚠️  candidate_profile.json not found.")
    except Exception as e:
        print(f"⚠️  Error loading candidate_profile.json: {e}")

    try:
        if os.path.exists("resume.md"):
            with open("resume.md", "r") as f:
                resume_text = f.read()
        else:
            print("⚠️  resume.md not found.")
    except Exception as e:
        print(f"⚠️  Error loading resume.md: {e}")

    return f"--- CANDIDATE PREFERENCES ---\n{profile_text}\n\n--- CANDIDATE RESUME ---\n{resume_text}"


# Load once globally when the script starts
CANDIDATE_DATA = load_candidate_data()

# ==========================================
# SYSTEM INSTRUCTION FOR GEMINI
# ==========================================
SYSTEM_INSTRUCTION = """
You are an autonomous job evaluation engine. Compare the Job Description against the Candidate Information.
Output ONLY valid JSON. No markdown, no backticks, no preamble.

CRITICAL FIRST CHECK — IS THIS A REAL JOB PAGE?
Before any evaluation, check whether the content is an actual job description from a company's
careers page or ATS. If the content is any of the following, return tier "Reject" immediately
with the matching rejection_reason — do NOT attempt skill matching:
- A social media post (Facebook, Instagram, LinkedIn post, Telegram message, WhatsApp forward)
- A recruiter's referral link post or "drive" announcement
- A blog article, news article, or aggregator page about jobs
- A page that says "This job is no longer accepting applications" or similar closure message
- A page with mostly unrendered template syntax like [[ variable ]] or {{ variable }}
- A talent community / expression of interest form (not a real open role)
- Any page where the actual job requirements cannot be clearly read

{
  "tier": "A|B|C|Reject",
  "job_title": "string",
  "company": "string",
  "location": "string",
  "experience_required_min": 0,
  "experience_required_max": 1,
  "rejection_reason": null,

  "skill_evaluation": {
    "required_skills_in_jd": ["string"],
    "candidate_covers": ["string"],
    "essential_missing": ["string"],
    "learnable_missing": ["string"],
    "nice_to_have_missing": ["string"]
  },

  "resume_changes": [
    {
      "section": "string",
      "action": "add|reword",
      "what": "string",
      "where": "string",
      "why": "string"
    }
  ],

  "one_line_summary": "string"
}

Tier rules:
- Tier A: All required skills match. Experience clearly 0-2 years. Strong backend/Java alignment. No essential gaps.
- Tier B: Required skills mostly match. Minor learnable gaps only (Redis, React basics, RabbitMQ). Worth applying.
- Tier C: Partial match. Significant gaps but not disqualifying. Candidate decides.
- Reject: ANY essential missing skill. OR experience_required_max > 2. OR wrong tech stack. OR non-technical role. OR role type matches not_interested_in list. OR content is not a real job page (see CRITICAL FIRST CHECK above).

Skill classification rules:
- Essential missing: skills the JD requires production experience in (Kafka, Kubernetes, .NET, C#, Go, Rust, PHP). Cannot be learned in 2-3 weeks. Hard reject.
- Learnable missing: skills credibly picked up in 2-4 weeks (Redis basics, React basics, RabbitMQ basics). Flag, do not reject.
- Nice-to-have missing: listed as optional (GraphQL, ElasticSearch). Ignore in scoring.
- Skills listed under learning_this_week in the candidate profile count as known. Do not flag them as missing.

Experience rules:
- Extract the maximum years explicitly stated. If not stated, default to 1.
- If max > 2: tier must be Reject and rejection_reason must explain why.

Resume changes rules:
- NEVER suggest adding a skill the candidate already has. Check the resume carefully first.
- Every suggestion must name the exact section, exact bullet or line, and exact keyword to add or change.
- Reason must be ATS-specific: explain how this phrase appears in the JD and why the mismatch hurts ranking.
- Generic advice ("mention your projects") is not acceptable. Be hyper-specific.

Rejection reason: Populate only when tier is Reject. Leave null for A, B, C.
"""


# ==========================================
# STEP 1 — DOMAIN ALLOWLIST CHECK
# (replaces the old loose keyword signal check)
# ==========================================
# url_is_from_allowed_domain() is defined above near the ALLOWED_DOMAINS list.


# ==========================================
# STEP 2 — HTTP HEAD PRE-FILTER
# ==========================================
def url_is_live(url):
    try:
        response = requests.head(url, timeout=8, allow_redirects=True)
        if response.status_code in [200, 405]:
            return True
        print(f"⏭️  HEAD check dropped URL (status {response.status_code}): {url[:60]}")
        return False
    except Exception:
        return False


# ==========================================
# STEP 3 — CONTENT VALIDITY CHECK
#
# Three things are checked:
# 1. Minimum length (500 chars — raised from 150).
#    150 chars let Facebook posts and referral links
#    through. 500 is the minimum for a real JD.
# 2. Unrendered JS template syntax.
#    If TinyFish fetched a page before Angular/Vue
#    hydrated it, the content will be full of
#    [[ variable ]] or {{ variable }} placeholders.
#    That is not a real job description.
# 3. Job-closed signal.
#    If the page explicitly says the role is closed,
#    there is nothing to apply to.
# ==========================================
# Regex to detect unrendered client-side template tokens
_JS_TEMPLATE_RE = re.compile(r'\[\[.{1,60}?\]\]|\{\{.{1,60}?\}\}')

# Phrases that indicate the listing is closed
_CLOSED_PHRASES = [
    "no longer accepting applications",
    "this job has expired",
    "position has been filled",
    "job is closed",
    "listing is no longer active",
    "applications are closed",
]

def content_is_valid(text):
    """
    Returns (is_valid: bool, reason: str).
    Callers check is_valid and use reason only for logging.
    """
    stripped = text.strip()

    # Check 1 — minimum length
    if len(stripped) < 500:
        return False, "too short (< 500 chars) — likely a login wall or empty page"

    text_lower = stripped.lower()

    # Check 2 — unrendered JS templates
    template_hits = _JS_TEMPLATE_RE.findall(stripped)
    if len(template_hits) >= 3:
        return False, f"unrendered JS template ({len(template_hits)} template tokens found) — page fetched before hydration"

    # Check 3 — job is closed
    for phrase in _CLOSED_PHRASES:
        if phrase in text_lower:
            return False, f"listing is closed ('{phrase}' found in content)"

    return True, "ok"


# ==========================================
# DUAL DEDUPLICATION HASH FUNCTIONS
# ==========================================
def generate_url_hash(url):
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def generate_company_title_hash(company, title):
    month_key = datetime.datetime.utcnow().strftime("%Y-%m")
    raw = f"{str(company).lower().strip()}{str(title).lower().strip()}{month_key}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ==========================================
# TINYFISH API WRAPPERS
# ==========================================
def tinyfish_search(query, page):
    url = "https://api.search.tinyfish.ai"
    headers = {"X-API-Key": TINYFISH_API_KEY}
    params = {"query": query, "num": 10, "page": page}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("results", [])
        print(f"⚠️  Search failed (status {response.status_code}) for: {query[:60]}")
    except Exception as e:
        print(f"❌  Search request error: {e}")
    return []


def tinyfish_fetch(url_to_fetch):
    url = "https://api.fetch.tinyfish.ai"
    headers = {"X-API-Key": TINYFISH_API_KEY}
    payload = {"urls": [url_to_fetch], "format": "markdown"}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=20)
        if response.status_code == 200:
            results = response.json().get("results", [])
            if results:
                return results[0].get("text", "")
    except Exception as e:
        print(f"❌  Fetch request error: {e}")
    return ""


# ==========================================
# QUOTA ALERT — once per calendar day (UTC)
# ==========================================
def _send_quota_alert_once():
    """Send a Telegram quota-exhausted alert at most once per calendar day (UTC).
    Checks MongoDB before sending — if an alert already went out today, stays silent."""
    today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    doc = meta_col.find_one({"_id": "quota_alert"})

    if doc and doc.get("last_sent_date") == today_str:
        print("📵  Quota alert already sent today — staying silent.")
        return

    bookmark = get_query_bookmark()
    send_telegram_notification(
        f"🚨 *Gemini API Quota Exhausted*\n\n"
        f"All {len(GEMINI_KEY_POOL)} key(s) hit their daily limit.\n"
        f"Pipeline paused at query #{bookmark}.\n"
        f"Will resume automatically after midnight UTC when limits reset."
    )

    meta_col.update_one(
        {"_id": "quota_alert"},
        {"$set": {"last_sent_date": today_str}},
        upsert=True
    )
    print(f"📬  Quota alert sent and recorded for {today_str}.")


# ==========================================
# GEMINI EVALUATION
# ==========================================
def evaluate_with_gemini(jd_text):
    client = get_ai_client()

    prompt = f"Candidate Information:\n{CANDIDATE_DATA}\n\nJob Description:\n{jd_text}"
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.2,
            ),
        )
        raw_text = response.text.strip()

        if raw_text.startswith("```json"):
            raw_text = raw_text[7:].strip()
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3].strip()
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:].strip()
            if raw_text.endswith("```"):
                raw_text = raw_text[:-3].strip()

        return json.loads(raw_text)

    except APIError as api_err:
        if api_err.code == 429:
            print(f"🚨 Quota hit on key #{_current_key_index}.")
            rotated = rotate_key()
            if rotated:
                print("🔄 Retrying with next key...")
                time.sleep(2)
                return evaluate_with_gemini(jd_text)  # Recursive retry — safe, set prevents infinite loop
            else:
                print("🚨  CRITICAL: All Gemini API keys exhausted!")
                _send_quota_alert_once()
                return "QUOTA_EXHAUSTED"

        print(f"⚠️  Gemini API error (code {api_err.code}): {api_err}")
    except json.JSONDecodeError as e:
        print(f"⚠️  Gemini returned non-JSON: {e}")
    except Exception as e:
        print(f"⚠️  Gemini processing error: {e}")
    return None


# ==========================================
# TELEGRAM NOTIFICATION HELPERS
# ==========================================
def send_telegram_notification(message, job_url=None, job_hash=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  Telegram credentials missing. Skipping alert.")
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }
    if job_url and job_hash:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "✅ Applied", "callback_data": f"applied:{job_hash}"},
                {"text": "❌ Skip",    "callback_data": f"skip:{job_hash}"},
            ]]
        }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            return response.json().get("result", {}).get("message_id")
        print(f"⚠️  Telegram returned status {response.status_code}")
    except Exception as e:
        print(f"❌  Failed to send Telegram message: {e}")
    return None


def build_telegram_message(tier_label, item, job_url, evaluation):
    skill_eval = evaluation.get("skill_evaluation", {})
    covers = skill_eval.get("candidate_covers", [])
    learnable = skill_eval.get("learnable_missing", [])
    essential = skill_eval.get("essential_missing", [])

    covers_str    = ", ".join(covers)    if covers    else "—"
    learnable_str = ", ".join(learnable) if learnable else None
    essential_str = ", ".join(essential) if essential else None

    tier = evaluation.get("tier", "")
    location = evaluation.get("location", "")
    location_line = f"\n📍 {location}" if location else ""

    msg = (
        f"*{tier_label}*\n\n"
        f"💼 {evaluation.get('job_title', item.get('title', '—'))} @ "
        f"{evaluation.get('company', item.get('site_name', '—'))}"
        f"{location_line}\n\n"
        f"✅ *You cover:* {covers_str}\n"
    )

    if learnable_str and tier in ("A", "B"):
        msg += f"⚡ *Learnable gap:* {learnable_str}\n"

    if essential_str and tier == "C":
        msg += f"⚠️ *Notable gaps:* {essential_str}\n"

    msg += f"\n💬 {evaluation.get('one_line_summary', '—')}\n\n"
    msg += f"[Apply Here]({job_url})"

    changes = evaluation.get("resume_changes", [])
    if changes:
        lines = []
        for c in changes:
            if isinstance(c, dict):
                section = c.get("section", "?")
                what    = c.get("what", "?")
                why     = c.get("why", "")
                lines.append(f"• [{section}] {what} — {why}")
            else:
                lines.append(f"• {c}")
        msg += "\n\n📝 *ATS Resume Tweaks:*\n" + "\n".join(lines)

    return msg


# ==========================================
# MONGODB WRITE HELPERS
# ==========================================
def mark_seen(url_hash, ct_hash, now):
    hash_doc = {
        "seen_at":    now,
        "expires_at": now + datetime.timedelta(days=180),
    }
    seen_hashes_col.update_one({"_id": url_hash}, {"$set": hash_doc}, upsert=True)
    seen_hashes_col.update_one({"_id": ct_hash},  {"$set": hash_doc}, upsert=True)


def save_job(url_hash, ct_hash, job_url, title, company, tier,
             evaluation, jd_markdown, telegram_message_id, now):
    job_doc = {
        "_id":                  url_hash,
        "url":                  job_url,
        "company_title_hash":   ct_hash,
        "telegram_message_id":  telegram_message_id,
        "job_title":            evaluation.get("job_title", title),
        "company":              evaluation.get("company", company),
        "location":             evaluation.get("location", ""),
        "tier":                 tier,
        "jd_text":              jd_markdown,
        "resume_changes":       evaluation.get("resume_changes", []),
        "one_line_summary":     evaluation.get("one_line_summary", ""),
        "action":               None,
        "saved_at":             now,
        "expires_at":           now + datetime.timedelta(days=14),
    }
    jobs_col.update_one({"_id": url_hash}, {"$set": job_doc}, upsert=True)


# ==========================================
# MAIN PIPELINE
# ==========================================
def run_pipeline():
    print("🚀 Starting Job Hunter Pipeline...")
    print(f"📋 Candidate data loaded: {len(CANDIDATE_DATA)} characters")
    print(f"🗂️  Query bank: {len(QUERY_BANK)} queries")

    RUN_START = time.time()
    SOFT_TIMEOUT_SECONDS = 25 * 60  # 25 minutes

    start_index = get_query_bookmark()
    if start_index > 0:
        print(f"📌 Resuming from query #{start_index} (bookmark found).")
    else:
        print("🆕 Starting from the beginning.")

    quota_tripped = False
    soft_timeout_hit = False

    for query_index, query in enumerate(QUERY_BANK):
        if query_index < start_index:
            continue

        if quota_tripped or soft_timeout_hit:
            break

        if time.time() - RUN_START > SOFT_TIMEOUT_SECONDS:
            print(f"\n⏰ Soft timeout reached between queries. Saving bookmark at #{query_index} and exiting cleanly.")
            save_query_bookmark(query_index)
            soft_timeout_hit = True
            break

        page = 0
        urls_seen_this_query = set()

        print(f"\n🔍 [{query_index + 1}/{len(QUERY_BANK)}] Query: '{query[:80]}...'")

        while True:
            if time.time() - RUN_START > SOFT_TIMEOUT_SECONDS:
                print(f"\n⏰ Soft timeout reached mid-query. Saving bookmark at #{query_index} and exiting cleanly.")
                save_query_bookmark(query_index)
                soft_timeout_hit = True
                break

            print(f"📄 Scanning page {page}...")
            results = tinyfish_search(query, page=page)

            if not results:
                print("🏁 End of index for this query.")
                break

            current_page_urls = {item.get("url") for item in results if item.get("url")}
            if urls_seen_this_query and current_page_urls:
                overlap = len(current_page_urls & urls_seen_this_query)
                overlap_pct = overlap / len(current_page_urls)
                if overlap_pct >= 0.8:
                    print(f"🔁 Pagination loop detected ({overlap}/{len(current_page_urls)} URLs repeat). Breaking.")
                    break
            urls_seen_this_query.update(current_page_urls)

            time.sleep(2)
            print(f"📥 {len(results)} URLs returned.")

            for item in results:
                if time.time() - RUN_START > SOFT_TIMEOUT_SECONDS:
                    print(f"\n⏰ Soft timeout reached mid-page. Saving bookmark at #{query_index} and exiting cleanly.")
                    save_query_bookmark(query_index)
                    soft_timeout_hit = True
                    break

                job_url = item.get("url")
                if not job_url:
                    continue

                title   = item.get("title", "")
                company = item.get("site_name", "")

                # ── GATE 1: domain allowlist ──────────────────────────────
                # Drop Facebook posts, Instagram, Telegram channels, blogs,
                # and any other non-job-platform URL before touching it.
                if not url_is_from_allowed_domain(job_url):
                    print(f"🚫 Blocked domain, skipping: {job_url[:80]}")
                    continue

                # ── GATE 2: deduplication ─────────────────────────────────
                url_hash = generate_url_hash(job_url)
                if seen_hashes_col.find_one({"_id": url_hash}):
                    continue

                ct_hash = generate_company_title_hash(company, title)
                if seen_hashes_col.find_one({"_id": ct_hash}):
                    print(f"⏭️  Duplicate job (different URL, same company+title): {title[:50]}")
                    now = datetime.datetime.utcnow()
                    seen_hashes_col.update_one(
                        {"_id": url_hash},
                        {"$set": {
                            "seen_at":    now,
                            "expires_at": now + datetime.timedelta(days=180),
                        }},
                        upsert=True,
                    )
                    continue

                # ── GATE 3: HTTP liveness ─────────────────────────────────
                if not url_is_live(job_url):
                    now = datetime.datetime.utcnow()
                    seen_hashes_col.update_one(
                        {"_id": url_hash},
                        {"$set": {
                            "seen_at":    now,
                            "expires_at": now + datetime.timedelta(days=180),
                        }},
                        upsert=True,
                    )
                    continue

                print(f"✨ New job: {title[:60]} — fetching content...")

                jd_markdown = tinyfish_fetch(job_url)

                # ── GATE 4: content validity ──────────────────────────────
                valid, reason = content_is_valid(jd_markdown)
                if not valid:
                    print(f"⏭️  Content invalid — {reason}")
                    now = datetime.datetime.utcnow()
                    seen_hashes_col.update_one(
                        {"_id": url_hash},
                        {"$set": {
                            "seen_at":    now,
                            "expires_at": now + datetime.timedelta(days=180),
                        }},
                        upsert=True,
                    )
                    continue

                # ── GATE 5: Gemini evaluation ─────────────────────────────
                print("🧠 Evaluating with Gemini...")
                evaluation = evaluate_with_gemini(jd_markdown)

                if evaluation == "QUOTA_EXHAUSTED":
                    save_query_bookmark(query_index)
                    quota_tripped = True
                    break

                if evaluation is None:
                    now = datetime.datetime.utcnow()
                    mark_seen(url_hash, ct_hash, now)
                    time.sleep(4.5)
                    continue

                exp_max = evaluation.get("experience_required_max", 1)
                if exp_max > 2:
                    print(f"⏩ Code-level reject: role requires {exp_max} years (hard limit: 2).")
                    evaluation["tier"] = "Reject"

                raw_location = evaluation.get("location", "")
                if not location_is_acceptable(raw_location):
                    print(f"⏩ Code-level reject: location outside India/Remote — '{raw_location}'.")
                    evaluation["tier"] = "Reject"

                tier = evaluation.get("tier")
                now  = datetime.datetime.utcnow()

                if tier in ("A", "B", "C"):
                    tier_label = TIER_LABEL.get(tier, "🔔 New Match")
                    message    = build_telegram_message(tier_label, item, job_url, evaluation)
                    message_id = send_telegram_notification(
                        message, job_url=job_url, job_hash=url_hash
                    )

                    save_job(
                        url_hash, ct_hash, job_url, title, company,
                        tier, evaluation, jd_markdown, message_id, now
                    )
                    print(f"💾 Saved — Tier {tier}: {title[:50]}")

                else:
                    reason = evaluation.get("rejection_reason") or evaluation.get("one_line_summary", "")
                    print(f"🗑️  Rejected: {reason[:80]}")

                mark_seen(url_hash, ct_hash, now)
                save_query_bookmark(query_index)
                time.sleep(4.5)

            if quota_tripped or soft_timeout_hit:
                break

            page += 1

    if not quota_tripped and not soft_timeout_hit:
        reset_query_bookmark()
        print("\n🏁 Pipeline run complete. Bookmark reset to 0.")
    elif soft_timeout_hit:
        print(f"\n⏰ Run ended via soft timeout. Bookmark saved. Next run resumes cleanly.")
    else:
        print(f"\n⏸️  Pipeline paused at query #{start_index}. Next run will resume from there.")


if __name__ == "__main__":
    run_pipeline()
