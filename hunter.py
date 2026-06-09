import os
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
    global _current_key_index, _ai_client
    if _current_key_index is None:
        _current_key_index = get_active_key_index()
        _ai_client = genai.Client(api_key=GEMINI_KEY_POOL[_current_key_index])
        print(f"🔑 Gemini key #{_current_key_index} active.")
    return _ai_client

def rotate_key():
    """Called when 429 hits. Switches to next key and persists it."""
    global _current_key_index, _ai_client
    next_index = get_next_key_index(_current_key_index)
    
    if next_index == _current_key_index:
        return False # Only one key in pool
        
    print(f"🔄 Rotating Gemini key: #{_current_key_index} → #{next_index}")
    _current_key_index = next_index
    _ai_client = genai.Client(api_key=GEMINI_KEY_POOL[_current_key_index])
    save_active_key_index(_current_key_index)
    return True


# ==========================================
# QUERY BANK — 28 queries across all sources
# ==========================================
QUERY_BANK = [
    # ------------------------------------------
    # [A] GREENHOUSE — Global ATS (Primary)
    # ------------------------------------------
    'site:greenhouse.io "Java" "Spring Boot" "Backend" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager" -"Director"',
    'site:job-boards.greenhouse.io "Backend Engineer" "Java" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager"',
    'site:greenhouse.io "Java Developer" "fresher" OR "entry level" OR "0-1" -"Senior" -"Lead" -"Manager"',
    'site:greenhouse.io "Java" "Spring Boot" "Bangalore" OR "Hyderabad" OR "Pune" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:greenhouse.io "Software Engineer" "Java" "Backend" "2026" OR "fresher" -"Senior" -"Staff" -"Manager"',

    # ------------------------------------------
    # [B] LEVER — Product startups globally
    # ------------------------------------------
    'site:lever.co "Java" "Spring Boot" "Backend" -"Senior" -"Lead" -"Staff" -"Principal" -"Manager" -"Director"',
    'site:lever.co "Java Developer" "fresher" OR "entry level" OR "0-2 years" -"Senior" -"Lead" -"Manager"',
    'site:lever.co "Backend Engineer" "Java" "Bangalore" OR "Hyderabad" OR "Remote" -"Senior" -"Manager" -"Director"',

    # ------------------------------------------
    # [C] ASHBY — High-growth startups (newer ATS)
    # ------------------------------------------
    'site:jobs.ashbyhq.com "Java" "Spring Boot" "Backend" -"Senior" -"Lead" -"Staff" -"Manager" -"Director"',
    'site:jobs.ashbyhq.com "Backend Engineer" "Java" "entry level" OR "fresher" OR "0-1" -"Senior" -"Manager"',

    # ------------------------------------------
    # [D] WORKDAY — Large enterprises and MNCs
    # ------------------------------------------
    'site:myworkdayjobs.com "Java" "Spring Boot" "fresher" OR "entry level" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:myworkdayjobs.com "Java Backend" "India" -"Senior" -"Lead" -"Manager" -"Director" -"3+ years" -"5+ years"',

    # ------------------------------------------
    # [E] SKILL-SPECIFIC ROTATION
    # ------------------------------------------
    'site:greenhouse.io "Spring Data REST" OR "Spring Security" "Java" -"Senior" -"Lead" -"Manager"',
    'site:lever.co "Spring Security" "Java" "Backend" -"Senior" -"Lead" -"Manager" -"Director"',
    'site:greenhouse.io "microservices" "Java" "entry level" OR "fresher" OR "0-1" -"Senior" -"Lead" -"Manager"',
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
    'site:wellfound.com "Backend Engineer" "Java" "entry level" -"Senior" -"Lead" -"Principal"',
    'site:wellfound.com "Java Developer" "Spring Boot" "India" -"Senior" -"Staff"',
    'site:internshala.com "Java" "Spring Boot" "backend" -"Senior"',

    # ------------------------------------------
    # [H] COMPANY CAREER SITES — Direct sourcing
    # ------------------------------------------
    'site:careers.razorpay.com "Java" "Backend" -"Senior" -"Lead" -"Manager"',
    'site:careers.phonepe.com "Java" "Backend" -"Senior" -"Lead" -"Manager"',
    'site:careers.paytm.com "Java" "Spring Boot" -"Senior" -"Lead" -"Manager"',
    'site:careers.flipkart.com "Java" "Backend Engineer" -"Senior" -"Staff" -"Lead" -"Manager"',
    'site:careers.swiggy.com "Java" "Backend" -"Senior" -"Lead" -"Manager"',
    'site:careers.cred.club "Java" "Backend" -"Senior" -"Lead" -"Manager"',

    'site:careers.infosys.com "Java" "Spring Boot" "fresher" 2026',
    'site:careers.wipro.com "Java" "Spring Boot" "fresher" 2026',

    'site:amazon.jobs "Java" "Software Engineer" "India" "entry level" OR "fresher" -"Senior" -"Principal" -"Manager"',
    'site:careers.walmart.com "Java" "Backend" "India" -"Senior" -"Lead" -"Manager" -"Director"',

    'site:careers.google.com "Java" "Software Engineer" "India" -"Senior" -"Staff" -"Principal" -"Manager"',
    'site:careers.microsoft.com "Java" "Software Engineer" "India" -"Senior" -"Principal" -"Manager" -"Director"',

    'site:goldmansachs.com/careers "Java" "Software Engineer" "India" -"Senior" -"Vice President" -"Managing Director"',
    'site:morganstanley.com/people/careers "Java" "Software Engineer" "India" -"Senior" -"Manager" -"Director"',
]

# URL signal keywords — any URL missing all of these is not a job page.
# Step 1 pre-filter: saves a TinyFish Fetch call on garbage navigation URLs.
JOB_URL_SIGNALS = [
    "job", "career", "careers", "position", "opening", "vacancy",
    "apply", "hiring", "greenhouse.io", "lever.co", "ashbyhq",
    "workday", "naukri", "linkedin.com/jobs", "instahyre", "wellfound"
]

# Tier display labels for Telegram notifications
TIER_LABEL = {
    "A": "🔥 STRONG MATCH — Apply Today",
    "B": "✅ GOOD MATCH — Worth Applying",
    "C": "👀 BORDERLINE — Your Call"
}

# ==========================================
# DYNAMIC CANDIDATE DATA LOADER
# Reads resume.md + candidate_profile.json at runtime.
# Update those files when resume or goals change — no code changes ever needed.
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
- Reject: ANY essential missing skill. OR experience_required_max > 2. OR wrong tech stack. OR non-technical role. OR role type matches not_interested_in list.

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
# STEP 1 — URL SIGNAL PRE-FILTER
# ==========================================
def url_has_job_signal(url):
    """Check the URL contains at least one job-related keyword.
    Saves a TinyFish Fetch call on navigation pages, category pages, and garbage URLs."""
    url_lower = url.lower()
    return any(signal in url_lower for signal in JOB_URL_SIGNALS)


# ==========================================
# STEP 2 — HTTP HEAD PRE-FILTER
# ==========================================
def url_is_live(url):
    """Fire a lightweight HEAD request. Drop 404s and redirects to homepages.
    Saves a TinyFish Fetch call on dead or expired job listings."""
    try:
        response = requests.head(url, timeout=8, allow_redirects=True)
        # Accept 200 and 405 (some servers reject HEAD but accept GET — still worth fetching)
        if response.status_code in [200, 405]:
            return True
        print(f"⏭️  HEAD check dropped URL (status {response.status_code}): {url[:60]}")
        return False
    except Exception:
        # Network error — skip gracefully, do not crash the pipeline
        return False


# ==========================================
# STEP 3 — CONTENT LENGTH CHECK
# ==========================================
def content_is_valid(text):
    """After TinyFish Fetch, check that the page has meaningful content.
    Under 150 chars = broken page, login wall, expired listing, or error page."""
    return len(text.strip()) >= 150


# ==========================================
# DUAL DEDUPLICATION HASH FUNCTIONS
# ==========================================
def generate_url_hash(url):
    """Hash 1 — Exact URL hash.
    Catches the same URL returned by multiple queries in the same run."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def generate_company_title_hash(company, title):
    """Hash 2 — Company + Title + Year-Month hash.
    Catches the same job posted simultaneously on Greenhouse, Naukri, and LinkedIn.
    Month component: a role re-opened 3+ months later is a new opportunity."""
    month_key = datetime.datetime.utcnow().strftime("%Y-%m")
    raw = f"{str(company).lower().strip()}{str(title).lower().strip()}{month_key}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ==========================================
# TINYFISH API WRAPPERS
# ==========================================
def tinyfish_search(query, page):
    """Search TinyFish and return the list of result objects for a query + page."""
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
    """Fetch a job page via TinyFish and return its Markdown content.
    TinyFish handles proxy rotation and Cloudflare/Akamai bypassing natively."""
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
# GEMINI EVALUATION
# ==========================================
def evaluate_with_gemini(jd_text):
    """Send JD + candidate data to Gemini. Return parsed JSON dict, or None on error,
    or the sentinel string 'QUOTA_EXHAUSTED' if the free tier is hit."""
    
    # Use dynamic round-robin client instead of a hardcoded one
    client = get_ai_client()
    
    prompt = f"Candidate Information:\n{CANDIDATE_DATA}\n\nJob Description:\n{jd_text}"
    try:
        response = client.models.generate_content(
            model="gemini-3.1-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                temperature=0.2,
            ),
        )
        raw_text = response.text.strip()

        # Strip any markdown fences if Gemini misbehaves
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
                return evaluate_with_gemini(jd_text) # Recursive retry
            else:
                print("🚨  CRITICAL: All Gemini API keys exhausted!")
                send_telegram_notification(
                    "🚨 *CRITICAL WARNING: Gemini API Quota Exhausted!*\n"
                    "All keys in the pool hit their daily limits."
                )
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
    """Send a Telegram message. If job_url and job_hash are provided, attach
    inline Applied / Skip buttons per the architecture spec."""
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
    # Inline keyboard only on actual job match notifications
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
            # Return message_id so we can store it for 5-day reminders (followup.py)
            return response.json().get("result", {}).get("message_id")
        print(f"⚠️  Telegram returned status {response.status_code}")
    except Exception as e:
        print(f"❌  Failed to send Telegram message: {e}")
    return None


def build_telegram_message(tier_label, item, job_url, evaluation):
    """Build the full structured Telegram message per the architecture spec."""
    skill_eval = evaluation.get("skill_evaluation", {})
    covers = skill_eval.get("candidate_covers", [])
    learnable = skill_eval.get("learnable_missing", [])
    essential = skill_eval.get("essential_missing", [])

    covers_str   = ", ".join(covers)   if covers   else "—"
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

    # ATS resume tweaks block
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
    """Mark both the URL hash and the company+title hash as seen in seen_hashes.
    TTL: 180 days (6 months), per architecture spec."""
    hash_doc = {
        "seen_at":    now,
        "expires_at": now + datetime.timedelta(days=180),
    }
    seen_hashes_col.update_one({"_id": url_hash}, {"$set": hash_doc}, upsert=True)
    seen_hashes_col.update_one({"_id": ct_hash},  {"$set": hash_doc}, upsert=True)


def save_job(url_hash, ct_hash, job_url, title, company, tier,
             evaluation, jd_markdown, telegram_message_id, now):
    """Write full job document to the jobs collection.
    TTL: 14 days. telegram_message_id stored for 5-day reminder logic in followup.py."""
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
        "action":               None,           # Set by followup.py when Applied/Skip pressed
        "saved_at":             now,
        "expires_at":           now + datetime.timedelta(days=14),   # TTL: 14 days
    }
    jobs_col.update_one({"_id": url_hash}, {"$set": job_doc}, upsert=True)


# ==========================================
# MAIN PIPELINE
# ==========================================
def run_pipeline():
    print("🚀 Starting Job Hunter Pipeline...")
    print(f"📋 Candidate data loaded: {len(CANDIDATE_DATA)} characters")
    print(f"🗂️  Query bank: {len(QUERY_BANK)} queries")
    quota_tripped = False

    for query in QUERY_BANK:
        if quota_tripped:
            break

        page = 0
        print(f"\n🔍 Query: '{query[:80]}...'")

        while True:
            print(f"📄 Scanning page {page}...")
            results = tinyfish_search(query, page=page)

            if not results:
                print("🏁 End of index for this query.")
                break

            # TinyFish search rate limit buffer: 30 RPM → 1 request per 2 seconds
            time.sleep(2)
            print(f"📥 {len(results)} URLs returned.")

            for item in results:
                job_url = item.get("url")
                if not job_url:
                    continue

                title   = item.get("title", "")
                company = item.get("site_name", "")

                # ── PRE-FILTER STEP 1: URL signal check ──────────────────────
                if not url_has_job_signal(job_url):
                    print(f"⏭️  No job signal in URL, skipping: {job_url[:60]}")
                    continue

                # ── DEDUPLICATION CHECK — URL hash ────────────────────────────
                url_hash = generate_url_hash(job_url)
                if seen_hashes_col.find_one({"_id": url_hash}):
                    continue

                # ── DEDUPLICATION CHECK — Company + Title + Month hash ────────
                ct_hash = generate_company_title_hash(company, title)
                if seen_hashes_col.find_one({"_id": ct_hash}):
                    print(f"⏭️  Duplicate job (different URL, same company+title): {title[:50]}")
                    # Also mark this URL hash seen so it is not re-checked
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

                # ── PRE-FILTER STEP 2: HTTP HEAD check ───────────────────────
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

                # ── TINYFISH FETCH ────────────────────────────────────────────
                jd_markdown = tinyfish_fetch(job_url)

                # ── PRE-FILTER STEP 3: Content length check ───────────────────
                if not content_is_valid(jd_markdown):
                    print("⏭️  Content too short or empty — login wall / expired listing.")
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

                # ── GEMINI EVALUATION ─────────────────────────────────────────
                print("🧠 Evaluating with Gemini...")
                evaluation = evaluate_with_gemini(jd_markdown)

                if evaluation == "QUOTA_EXHAUSTED":
                    quota_tripped = True
                    break

                if evaluation is None:
                    # Gemini parse error — mark seen to avoid wasting quota again
                    now = datetime.datetime.utcnow()
                    mark_seen(url_hash, ct_hash, now)
                    time.sleep(4.5)
                    continue

                # ── CODE-LEVEL EXPERIENCE ENFORCEMENT ────────────────────────
                exp_max = evaluation.get("experience_required_max", 1)
                if exp_max > 2:
                    print(f"⏩ Code-level reject: role requires {exp_max} years (hard limit: 2).")
                    evaluation["tier"] = "Reject"

                tier = evaluation.get("tier")
                now  = datetime.datetime.utcnow()

                if tier in ("A", "B", "C"):
                    # ── TELEGRAM NOTIFICATION ─────────────────────────────────
                    tier_label = TIER_LABEL.get(tier, "🔔 New Match")
                    message    = build_telegram_message(tier_label, item, job_url, evaluation)
                    message_id = send_telegram_notification(
                        message, job_url=job_url, job_hash=url_hash
                    )

                    # ── SAVE TO MONGODB — jobs collection ─────────────────────
                    save_job(
                        url_hash, ct_hash, job_url, title, company,
                        tier, evaluation, jd_markdown, message_id, now
                    )
                    print(f"💾 Saved — Tier {tier}: {title[:50]}")

                else:
                    reason = evaluation.get("rejection_reason") or evaluation.get("one_line_summary", "")
                    print(f"🗑️  Rejected: {reason[:80]}")

                # ── MARK BOTH HASHES SEEN ─────────────────────────────────────
                mark_seen(url_hash, ct_hash, now)

                # Gemini rate limit: 15 RPM → sleep 4.5 seconds between calls
                time.sleep(4.5)

            page += 1

    print("\n🏁 Pipeline run complete.")


if __name__ == "__main__":
    run_pipeline()
