import json
import boto3
import urllib.request
import urllib.error
import urllib.parse
import logging
import re
import os
import time
import difflib
import random
import unicodedata
from datetime import date
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    from pypdf import PdfReader   # bundled into the deploy zip by .github/workflows/deploy.yml
except ImportError:
    PdfReader = None

HIIVE_ASK_FIELD       = "custom_label_3997297"
HIIVE_BID_FIELD       = "custom_label_3997298"
HIIVE_ASK_DATE_FIELD  = "custom_label_3997299"
HIIVE_BID_DATE_FIELD  = "custom_label_3997300"

# ── Load agent data from S3 at cold start ─────────────────────────────────────
def _load_agent_data():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="pipeline-token", Key="agent-data.json")
    return json.loads(obj['Body'].read())

_AGENT_DATA     = _load_agent_data()
SECURITY_IDS    = _AGENT_DATA["security_ids"]
_DEAL_FIELDS    = _AGENT_DATA["deal_fields"]

INDUSTRY_MAP = {
    "Advanced Materials": 5080477, "Advertising": 6041686, "Aerospace": 6291286,
    "AgTech": 6411460, "AI": 5079682, "Analytics": 6323896, "Apps": 6041689,
    "Augmented Reality": 6497595, "Autonomous Vehicles": 6041692, "B2B": 6498389,
    "B2C": 6498390, "Batteries": 5080480, "Big Data": 5079691, "BioTech": 5079685,
    "Business Intelligence": 7041664, "Cannabis": 6348286, "Construction": 6559224,
    "Consumer": 6692138, "CRM": 6548509, "Crypto/Blockchain": 6437357,
    "Cybersecurity": 5080471, "Database": 6543471, "Defense": 6773331,
    "Delivery": 6558585, "Design": 6584819, "Developer Tools": 6584820,
    "Drones": 6323887, "E-Commerce": 5079694, "EdTech": 5639584,
    "Education": 6560147, "Electric Vehicles": 6925054, "Electronics": 6925055,
    "Energy": 5099446, "Fashion/Retail": 6874488, "Finance": 6348289,
    "FinTech": 5079676, "Food Delivery": 6687285, "Food&Beverage": 5622097,
    "FoodTech": 6411463, "Funding Platform": 6681863, "Gaming": 6589044,
    "GreenTech": 7005277, "Hardware": 6437358, "Healthcare": 5622091,
    "Human Resources": 6661451, "IaaS": 5891584, "Impact Investing": 6771364,
    "Information Services": 5865601, "Infrastructure": 6323899, "Insurance": 6558456,
    "Internet": 6755652, "IoT": 6687287, "IT": 6538857, "Legal": 6668597,
    "Lifestyle": 5079697, "Logistics": 6674874, "Machine Learning": 6323902,
    "Management": 6323905, "Manufacturing": 6322129, "Marketing": 6041695,
    "Marketplace": 6674875, "Media": 6322132, "Mining": 6774700,
    "Movies, Music and Entertainment": 6506196, "NanoTech": 6925056,
    "Oil & Gas": 6755654, "Packaging": 6876091, "Pharma": 6755653,
    "Publishing": 6563263, "Real Estate": 6446568,
    "Renewables / Clean Energy": 5527627, "Resources": 6771365,
    "Restaurant": 6973702, "Robotics": 6323890, "Saas": 6041698,
    "Sharing Economy": 5141053, "Social Media": 5079688, "Software": 5080483,
    "Sports": 6751585, "SRI / ESG": 6438707, "Technology": 6323893,
    "Telecom": 5080474, "TMT": 6543472, "Transport": 5079679, "Travel": 6323884,
    "Utilities": 6755655, "Virtual Reality": 6774699, "Water": 5527624,
    "Web3": 6786914, "Wellness": 6726243, "PropTech": 7129933,
}

def _load_system_prompt():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="pipeline-token", Key="agent-system-prompt.txt")
    return obj['Body'].read().decode('utf-8')

_SYSTEM_PROMPT  = _load_system_prompt()




logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Load JWT from S3 ──────────────────────────────────────────────────────────
def get_jwt_from_s3():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="pipeline-token", Key="pipeline-jwt.json")
    data = json.loads(obj['Body'].read())
    return data['jwt']

# ── Load leads cache from S3 ──────────────────────────────────────────────────
def get_leads_cache():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="fetched-leads", Key="leads_data.json")
    data = json.loads(obj['Body'].read())
    return data.get('leads', [])

# ── Load deals cache from S3 ─────────────────────────────────────────────────
def get_deals_cache():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="fetched-leads", Key="deals_data.json")
    data = json.loads(obj['Body'].read())
    return data.get('deals', [])

# ── Full snapshot loaders (pipeline-cache bucket) ─────────────────────────────
_snapshot_cache = {}

def get_snapshot(key):
    """Load a snapshot file from full-pipeline-cache, cached in memory for this execution."""
    if key not in _snapshot_cache:
        s3 = boto3.client('s3')
        try:
            obj = s3.get_object(Bucket="full-pipeline-cache", Key=key)
            data = json.loads(obj['Body'].read())
            record_key = key.replace('.json', '')  # people, companies, deals
            _snapshot_cache[key] = data.get(record_key, [])
            logger.info(f"Loaded snapshot {key}: {len(_snapshot_cache[key])} records")
        except Exception as e:
            logger.error(f"Failed to load snapshot {key}: {e}")
            _snapshot_cache[key] = []
    return _snapshot_cache[key]

# ── Live-API refresh for SECURITY_IDS ─────────────────────────────────────────
# Roles map Pipeline custom-field IDs to the role keys used in SECURITY_IDS.
_SECURITY_FIELD_ROLES = {3322093: "b", 3759156: "s", 3740611: "h"}

def _refresh_security_ids_from_api():
    """Fetch the live person custom-field dropdowns for the three security
    interest fields and merge entries into module-level SECURITY_IDS.
    Returns True on success, False on failure (logged, never raises)."""
    try:
        result = call_pipeline_api(
            "GET",
            "/admin/custom_field_labels.json?conditions[entity_type]=person",
        )
    except Exception as e:
        logger.error(f"_refresh_security_ids_from_api: request failed: {e}")
        return False
    if result.get("status") != 200:
        logger.error(
            f"_refresh_security_ids_from_api: status={result.get('status')} "
            f"data={str(result.get('data'))[:200]}"
        )
        return False
    body = result.get("data") or {}
    if isinstance(body, dict):
        labels = (body.get("custom_field_labels")
                  or body.get("entries")
                  or body.get("data")
                  or [])
    elif isinstance(body, list):
        labels = body
    else:
        labels = []
    merged = {}
    for label in labels:
        if not isinstance(label, dict):
            continue
        lid = label.get("id")
        role = _SECURITY_FIELD_ROLES.get(lid)
        if not role:
            continue
        entries = label.get("custom_field_label_dropdown_entries") or []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            eid = entry.get("id")
            if not name or not eid:
                continue
            slot = merged.setdefault(name, {"h": None, "b": None, "s": None})
            slot[role] = int(eid)
    if not merged:
        logger.error("_refresh_security_ids_from_api: no security entries parsed")
        return False
    for name, ids in merged.items():
        SECURITY_IDS[name] = ids
    logger.info(
        f"_refresh_security_ids_from_api: refreshed {len(merged)} security entries"
    )
    return True

# ── Form-status downgrade guard ───────────────────────────────────────────────
# IQF / CEF Submitted are set when a form is received and must never be
# downgraded by the agent. Rank each option; an incoming value that ranks <= the
# current value (and differs from it) is dropped from the update payload.
FORM_STATUS_RANKS = {
    "custom_label_3763008": ("IQF Submitted", {6496840: 3, 6496842: 2, 6596073: 1, 6496841: 1},
                             {6496840: "Yes", 6496842: "Pending", 6596073: "Unnecessary", 6496841: "No"}),
    "custom_label_3796440": ("CEF Submitted", {6600515: 3, 6600514: 2, 6600516: 1, 6600513: 1},
                             {6600515: "Yes", 6600514: "Pending", 6600516: "N/A", 6600513: "No"}),
}

def _single_option_id(v):
    if isinstance(v, list):
        v = v[0] if v else None
    if isinstance(v, dict):
        v = v.get("id")
    if v is None or isinstance(v, bool):
        return None
    s = str(v).strip()
    return int(s) if s.isdigit() else None

def guard_form_status_downgrades(person_id, custom):
    """Drop IQF/CEF keys from `custom` that would downgrade the live value.
    Returns a list of human-readable blocked-downgrade messages."""
    keys = [k for k in FORM_STATUS_RANKS if k in custom]
    if not keys:
        return []
    blocked = []
    existing = call_pipeline_api("GET", f"/people/{person_id}.json")
    if existing["status"] != 200 or not isinstance(existing["data"], dict):
        # Fail open on the rest of the update, closed on these fields.
        logger.warning(f"form-status guard: could not fetch person {person_id} "
                       f"(status {existing['status']}); dropping {keys} from update")
        for k in keys:
            label = FORM_STATUS_RANKS[k][0]
            custom.pop(k, None)
            blocked.append(f"{label}: not changed (could not verify current value)")
        return blocked
    existing_cf = existing["data"].get("custom_fields") or {}
    for k in keys:
        label, ranks, names = FORM_STATUS_RANKS[k]
        cur = _single_option_id(existing_cf.get(k))
        new = _single_option_id(custom[k])
        if new == cur:
            continue
        if ranks.get(new, 0) <= ranks.get(cur, 0):
            cur_name = names.get(cur, "blank" if cur is None else str(cur))
            new_name = names.get(new, "blank" if new is None else str(new))
            custom.pop(k)
            msg = f"{label}: kept {cur_name} (attempted {new_name})"
            logger.warning(f"form-status guard: person {person_id} — {msg}")
            blocked.append(msg)
    return blocked

# ── Phone normalization (every person/company write goes through this) ────────
# Country → calling code, keyed by lowercased name and ISO2. Used only as a hint for
# numbers written without "+"/"00"; a number with an explicit international prefix
# never needs it.
_CALLING_CODES = {
    "us": "1", "usa": "1", "united states": "1", "united states of america": "1",
    "ca": "1", "canada": "1", "gb": "44", "uk": "44", "united kingdom": "44", "england": "44",
    "ie": "353", "ireland": "353", "fr": "33", "france": "33", "de": "49", "germany": "49",
    "ch": "41", "switzerland": "41", "nl": "31", "netherlands": "31", "be": "32", "belgium": "32",
    "lu": "352", "luxembourg": "352", "es": "34", "spain": "34", "pt": "351", "portugal": "351",
    "it": "39", "italy": "39", "at": "43", "austria": "43", "se": "46", "sweden": "46",
    "no": "47", "norway": "47", "dk": "45", "denmark": "45", "fi": "358", "finland": "358",
    "pl": "48", "poland": "48", "ua": "380", "ukraine": "380", "gr": "30", "greece": "30",
    "cy": "357", "cyprus": "357", "mt": "356", "malta": "356", "mc": "377", "monaco": "377",
    "il": "972", "israel": "972", "tr": "90", "turkey": "90", "ae": "971", "uae": "971",
    "united arab emirates": "971", "sa": "966", "saudi arabia": "966", "qa": "974", "qatar": "974",
    "kw": "965", "kuwait": "965", "bh": "973", "bahrain": "973", "om": "968", "oman": "968",
    "eg": "20", "egypt": "20", "za": "27", "south africa": "27", "ng": "234", "nigeria": "234",
    "ke": "254", "kenya": "254", "in": "91", "india": "91", "pk": "92", "pakistan": "92",
    "sg": "65", "singapore": "65", "hk": "852", "hong kong": "852", "cn": "86", "china": "86",
    "tw": "886", "taiwan": "886", "jp": "81", "japan": "81", "kr": "82", "south korea": "82",
    "korea": "82", "th": "66", "thailand": "66", "my": "60", "malaysia": "60",
    "id": "62", "indonesia": "62", "ph": "63", "philippines": "63", "vn": "84", "vietnam": "84",
    "au": "61", "australia": "61", "nz": "64", "new zealand": "64", "br": "55", "brazil": "55",
    "mx": "52", "mexico": "52", "ar": "54", "argentina": "54", "cl": "56", "chile": "56",
    "co": "57", "colombia": "57", "pe": "51", "peru": "51",
}
PHONE_FIELDS = ("phone", "mobile", "work_phone", "home_phone")
COMPANY_ADDRESS_FIELDS = ("address_1", "address_2", "city", "state", "postal_code", "country")

def _calling_code(country):
    return _CALLING_CODES.get(re.sub(r"[^a-z ]", "", (country or "").lower()).strip())

def normalize_phone(raw, country=None):
    """E.164 for `raw`, or (None, reason) when it can't be done without losing digits.
    Returns (e164, None) on success. `country` (name or ISO2) is only a hint for numbers
    written with a national trunk 0, e.g. "(0971) 585-629481" + UAE → +971585629481."""
    s = re.sub(r"\(\s*0\s*\)", "", (raw or "").strip())   # "+44 (0) 20…": the (0) is never dialled
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None, f"no digits in {raw!r}"
    cc = _calling_code(country)
    if s.startswith("+") and not digits.startswith("0"):
        out = digits
    elif digits.startswith("00"):
        out = digits[2:]
    elif cc and digits.startswith("0" + cc) and len(digits) - 1 - len(cc) >= 6:
        out = digits[1:]                       # "0" + country code: trunk 0 typed before the CC
    elif cc and cc != "1" and digits.startswith(cc) and len(digits) >= 11 and len(digits) - len(cc) >= 8:
        out = digits
    elif cc and digits.startswith("0"):
        out = cc + digits[1:]                  # national number with trunk 0
    elif (cc in (None, "1")) and len(digits) == 10:
        out = "1" + digits
    elif (cc in (None, "1")) and len(digits) == 11 and digits.startswith("1"):
        out = digits
    elif cc:
        out = cc + digits
    else:
        return None, f"{raw!r} has no country code and no country to infer one from"
    # Lost-digit guard: every significant digit of the input (all but international
    # "00" / one trunk "0") must survive, in order, at the end of the output.
    significant = digits[2:] if digits.startswith("00") else digits[1:] if digits.startswith("0") else digits
    if not out.endswith(significant):
        return None, f"normalizing {raw!r} → +{out} would drop digits"
    if not 8 <= len(out) <= 15:
        return None, f"{raw!r} → +{out} is not a valid E.164 length ({len(out)} digits)"
    return "+" + out, None

def guard_phone_fields(fields, country=None, keys=PHONE_FIELDS):
    """Normalize phone values in `fields` in place. Unnormalizable values are removed
    (never written) and returned as flag messages for the reply."""
    flags = []
    for k in [k for k in fields if k in keys]:
        v = fields[k]
        if not isinstance(v, str) or not v.strip():
            continue
        e164, problem = normalize_phone(v, country)
        if e164:
            fields[k] = e164
        else:
            fields.pop(k)
            logger.warning(f"phone guard: {k} not written — {problem}")
            flags.append(f"{k} not written: {problem}")
    return flags

# ── Entity forms (IQF for Legal Entities / entity CEF) ────────────────────────
_ENTITY_FORM_RE = re.compile(
    r"principal\s+place\s+of\s+business|name\s+of\s+entity\s+client|entity\s+investor\s+name"
    r"|for\s+legal\s+entit(?:y|ies)", re.I)
PERSON_ADDRESS_FIELDS = {f"{p}_{s}" for p in ("work", "home")
                         for s in ("address_1", "address_2", "street", "city", "state", "postal_code", "country")}

def entity_form_trigger():
    """What marks the current email as an entity IQF/CEF, e.g. "attachment 'iqf.pdf': 'Principal
    Place of Business'", or None. Logged whenever the entity guard drops data."""
    em = _CURRENT_EMAIL or {}
    sources = [("subject", em.get("subject")), ("body", em.get("body"))]
    sources += [(f"attachment {a.get('filename', '?')!r}", _attachment_text(a)) for a in _CURRENT_ATTACHMENTS]
    for where, text in sources:
        m = _ENTITY_FORM_RE.search(text or "")
        if m:
            return f"{where}: {' '.join(m.group(0).split())!r}"
    return None

def _same_value(sent, saved):
    return " ".join(str(sent or "").split()).lower() == " ".join(str(saved or "").split()).lower()

def put_company_checked(company_id, payload, check_keys):
    """PUT the company, then confirm each of `check_keys` (address/phone) actually saved.
    A key missing from a GET is still sent — Pipeline omits null-valued keys. A key is
    reported unknown only if the PUT rejects it by name or the follow-up GET shows it
    didn't save. Returns (put_result, unknown_keys)."""
    url = f"/companies/{company_id}.json"
    std = payload["company"]
    unknown = []
    result = call_pipeline_api("PUT", url, payload)
    if result["status"] != 200:
        named = [k for k in check_keys if k in std and k in json.dumps(result.get("data"))]
        if not named:
            return result, unknown
        unknown += named
        for k in named:
            std.pop(k)
        if not std and not payload["company"].get("custom_fields"):
            return {"status": 200, "data": {}}, unknown
        result = call_pipeline_api("PUT", url, payload)
        if result["status"] != 200:
            return result, unknown
    sent = {k: std[k] for k in check_keys if k in std}
    if sent:
        after = call_pipeline_api("GET", url)
        if after["status"] == 200 and isinstance(after["data"], dict):
            unknown += [k for k, v in sent.items() if not _same_value(v, after["data"].get(k))]
        else:
            logger.warning(f"update_company: could not re-read company {company_id} to confirm {list(sent)}")
    if unknown:
        logger.warning(f"update_company: company {company_id} address/phone keys not saved: {unknown}")
    return result, unknown

# ── Call Pipeline CRM API ─────────────────────────────────────────────────────
def call_pipeline_api(method, endpoint, payload=None):
    base_url = "https://api.pipelinecrm.com/api/v3"
    url = f"{base_url}{endpoint}"
    jwt_token = get_jwt_from_s3()
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as response:
            return {"status": response.status, "data": json.loads(response.read().decode())}
    except urllib.error.HTTPError as e:
        try:
            err_data = e.read().decode()
        except:
            err_data = str(e)
        return {"status": e.code, "data": err_data}
    except Exception as e:
        return {"status": 500, "data": str(e)}

# ── Google Drive (client folders) ─────────────────────────────────────────────
DRIVE_CLIENTS_FOLDER_ID = "15tJGEiOe4eKszNHDLrm5wG_8C6Icn5wo"
DRIVE_CREDS_BUCKET      = "pipeline-token"
DRIVE_CREDS_KEY         = "google-drive-oauth.json"
DRIVE_FILES_URL         = "https://www.googleapis.com/drive/v3/files"

_drive_creds = None
_drive_token = {"access_token": None, "expires_at": 0}

def _drive_access_token():
    global _drive_creds
    if _drive_token["access_token"] and time.time() < _drive_token["expires_at"]:
        return _drive_token["access_token"]
    if _drive_creds is None:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=DRIVE_CREDS_BUCKET, Key=DRIVE_CREDS_KEY)
        _drive_creds = json.loads(obj['Body'].read())
    body = urllib.parse.urlencode({
        "client_id": _drive_creds["client_id"],
        "client_secret": _drive_creds["client_secret"],
        "refresh_token": _drive_creds["refresh_token"],
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=body,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as resp:
        tok = json.loads(resp.read().decode())
    _drive_token["access_token"] = tok["access_token"]
    _drive_token["expires_at"] = time.time() + int(tok.get("expires_in", 3600)) - 60
    return _drive_token["access_token"]

def _drive_raw(method, url, params=None, body=None, headers=None, _retried=False):
    """Low-level Drive call. Returns {"status", "data", "headers"}; data is parsed JSON when possible.
    Never raises. Retries once on 401 with a fresh token."""
    try:
        token = _drive_access_token()
    except urllib.error.HTTPError as e:
        try:
            err = e.read().decode()
        except Exception:
            err = str(e)
        return {"status": e.code, "data": f"OAuth token refresh failed: {err}", "headers": {}}
    except Exception as e:
        return {"status": 500, "data": f"OAuth token refresh failed: {e}", "headers": {}}
    full_url = f"{url}?{urllib.parse.urlencode(params)}" if params else url
    hdrs = dict(headers or {})
    hdrs["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(full_url, data=body, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode()
            try:
                data = json.loads(raw) if raw else {}
            except ValueError:
                data = raw
            return {"status": resp.status, "data": data, "headers": dict(resp.headers)}
    except urllib.error.HTTPError as e:
        if e.code == 401 and not _retried:
            _drive_token["access_token"] = None
            _drive_token["expires_at"] = 0
            return _drive_raw(method, url, params, body, headers, _retried=True)
        try:
            err = e.read().decode()
        except Exception:
            err = str(e)
        return {"status": e.code, "data": err, "headers": {}}
    except Exception as e:
        return {"status": 500, "data": str(e), "headers": {}}

def _drive_request(method, url, params=None, payload=None):
    """JSON Drive call. Returns {"status", "data"}. Never raises."""
    body, headers = None, {}
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    r = _drive_raw(method, url, params=params, body=body, headers=headers)
    return {"status": r["status"], "data": r["data"]}

def _drive_list(q, fields="nextPageToken,files(id,name)"):
    """Live, paginated files.list. Returns (files, error_result)."""
    files, page_token = [], None
    while True:
        params = {"q": q, "fields": fields, "pageSize": 1000}
        if page_token:
            params["pageToken"] = page_token
        r = _drive_request("GET", DRIVE_FILES_URL, params=params)
        if r["status"] != 200 or not isinstance(r["data"], dict):
            return None, r
        files.extend(r["data"].get("files", []))
        page_token = r["data"].get("nextPageToken")
        if not page_token:
            return files, None

def _list_client_folders():
    """Live listing of all folders directly under CLIENTS. Returns (folders, error_result)."""
    return _drive_list(f"'{DRIVE_CLIENTS_FOLDER_ID}' in parents and "
                       "mimeType='application/vnd.google-apps.folder' and trashed=false")

def _list_folder_files(folder_id):
    """Live listing of non-trashed, non-folder files directly in folder_id. Returns (files, error_result)."""
    return _drive_list(f"'{folder_id}' in parents and trashed=false and "
                       "mimeType!='application/vnd.google-apps.folder'",
                       fields="nextPageToken,files(id,name,mimeType)")

_ENTITY_SUFFIXES = {"llc", "inc", "incorporated", "ltd", "limited", "lp", "llp",
                    "corp", "corporation", "co", "gmbh", "sa", "ag", "plc"}

def _folder_tokens(name, drop_suffixes=True):
    s = (name or "").lower()
    s = re.sub(r"[-‐‑‒–—/]", " ", s)   # dashes/slashes separate words
    s = re.sub(r"[^\w\s]", "", s)      # other punctuation dropped: "l.l.c." -> "llc"
    tokens = s.split()
    while drop_suffixes and len(tokens) > 1 and tokens[-1] in _ENTITY_SUFFIXES:
        tokens.pop()
    return tokens

def _norm_folder_name(name):
    return " ".join(_folder_tokens(name))

def _person_folder_name(name):
    name = " ".join((name or "").split())
    if "," in name:
        return name
    parts = name.split(" ")
    if len(parts) < 2:
        return name
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

def _folder_result(status, f):
    return {"status": status, "folder_id": f["id"], "name": f["name"],
            "url": f"https://drive.google.com/drive/folders/{f['id']}"}

def _match_client_folders(client_name, client_type, folders):
    """Tiered matching; returns (matches, near_matches) from the first tier that yields any.
    1) normalized names equal  2) token sets equal (persons)  3) query tokens are a prefix of the folder's tokens
    4) near match, only if 1-3 found nothing: folder tokens are a prefix of the query's, or token-set
       Jaccard >= 0.6 with >= 2 shared tokens. Near matches are never treated as exists."""
    target = _folder_tokens(client_name)
    if not target:
        return [], []
    normed = [(f, _folder_tokens(f.get("name"))) for f in folders]
    tier1 = [f for f, t in normed if t == target]
    if tier1:
        return tier1, []
    if client_type == "person":
        tier2 = [f for f, t in normed if set(t) == set(target)]
        if tier2:
            return tier2, []
    prefixes = [target]
    if client_type == "person" and "," not in client_name and len(target) > 1:
        prefixes.append([target[-1]] + target[:-1])   # "marc meunier" -> also "meunier marc"
    tier3 = []
    for f in folders:
        t = _folder_tokens(f.get("name"), drop_suffixes=False)
        if any(t[:len(pre)] == pre for pre in prefixes):
            tier3.append(f)
    if tier3:
        return tier3, []
    near = []
    for f, t in normed:
        if not t:
            continue
        shared = set(t) & set(target)
        if target[:len(t)] == t or (len(shared) >= 2 and len(shared) / len(set(t) | set(target)) >= 0.6):
            near.append(f)
    return [], near

# ── Person folder resolution (identity clues + nicknames) ─────────────────────
# Person folders are resolved from the strongest identity clue on the email, not just the name the
# agent passes (which may be a nickname from a signature): 1) email addresses (sender / forwarded
# sender / attachment text) looked up in Pipeline, 2) full names in attachment file names and PDF
# text, 3) names in the email subject/body. Folders are then matched by last name + first name,
# nickname variant (names.csv, vendored from github.com/carltonnorthern/nicknames), or contents.

NICKNAMES_CSV       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "names.csv")
_INTERNAL_DOMAINS   = ("graciagroup.com", "rainmakersecurities.com", "amazonses.com", "amazonaws.com")
_NAME_SUFFIXES      = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "esq", "cpa", "cfa"}
_NOT_FIRST_NAMES    = {"mr", "mrs", "ms", "miss", "dr", "by", "and", "or", "the", "of", "to", "for", "from",
                       "signed", "name", "dear", "hi", "hello", "thanks", "re", "fw", "fwd", "attn", "per",
                       "with", "between", "seller", "buyer", "transferor", "transferee", "holder", "investor",
                       "signature", "print", "printed", "title", "date", "email", "cc", "bcc", "is", "as"}
_EMAIL_RE           = re.compile(r"[\w.+%-]+@[\w-]+(?:\.[\w-]+)*\.[a-z]{2,}", re.I)
_PDF_MAX_PAGES      = 30
_PDF_MAX_CHARS      = 200_000

# Email being processed (set by lambda_handler for SES invocations, reset on every invocation).
_CURRENT_EMAIL = {}
_nicknames = None

def _load_nicknames(path=None):
    """{name: set(names)} of direct nickname relations, both directions. Missing file → {}."""
    rel = {}
    try:
        with open(path or NICKNAMES_CSV, encoding="utf-8") as fh:
            for line in fh:
                parts = [x.strip().lower() for x in line.split(",")]
                if len(parts) != 3 or parts[1] != "has_nickname":
                    continue
                rel.setdefault(parts[0], set()).add(parts[2])
                rel.setdefault(parts[2], set()).add(parts[0])
    except OSError:
        pass
    return rel

def _nickname_map():
    global _nicknames
    if _nicknames is None:
        _nicknames = _load_nicknames()
        logger.info(f"Loaded nickname data: {len(_nicknames)} names")
    return _nicknames

def _name_tokens(s):
    """Lowercase ASCII word tokens: accents stripped, punctuation dropped, dashes split words."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[-‐‑‒–—/_]", " ", s)
    s = re.sub(r"[^\w\s,]", "", s)
    return s.replace(",", " , ").split()

def _split_person_name(name):
    """('first', ['last', ...]) from 'First [Middle] Last' or 'Last, First'. Suffixes dropped.
    A single token comes back as ('', [token])."""
    toks = [t for t in _name_tokens(name) if t not in _NAME_SUFFIXES]
    if "," in toks:
        i = toks.index(",")
        last = [t for t in toks[:i] if t != ","]
        first = [t for t in toks[i + 1:] if t != ","]
        if last and first:
            return first[0], last
        toks = last or first
    if not toks:
        return "", []
    if len(toks) == 1:
        return "", toks
    return toks[0], toks[-1:]

def _first_name_relation(a, b, nick):
    """'first name' if equal, 'nickname' if a direct nickname pair, else None."""
    a, b = (a or "").lower(), (b or "").lower()
    if not a or not b:
        return None
    if a == b:
        return "first name"
    if b in nick.get(a, ()) or a in nick.get(b, ()):
        return "nickname"
    return None

def _first_names_consistent(a, b, nick):
    """Looser check for 'is this clue about the same person': equal, nickname pair, or one is a
    3+ letter prefix of the other (Zach / Zachary)."""
    a, b = (a or "").lower(), (b or "").lower()
    if _first_name_relation(a, b, nick):
        return True
    short, long_ = sorted((a, b), key=len)
    return len(short) >= 3 and long_.startswith(short)

def _extract_emails(text):
    out = []
    for e in _EMAIL_RE.findall(text or ""):
        e = e.lower().strip(".")
        if e not in out:
            out.append(e)
    return out

def _is_internal_email(email):
    dom = email.rsplit("@", 1)[-1].lower()
    return any(dom == d or dom.endswith("." + d) for d in _INTERNAL_DOMAINS)

def _names_with_last(text, last):
    """First names written next to the last name in text: 'First [M.] Last' or 'Last, First'.
    Case-insensitive (signature blocks are often all caps). Returns lowercase first names in order."""
    if not text or not last:
        return []
    flat = unicodedata.normalize("NFKD", text)
    flat = "".join(c for c in flat if not unicodedata.combining(c))
    flat = re.sub(r"[_]", " ", flat)
    ln = r"[\s-]+".join(re.escape(t) for t in last)
    word = r"([A-Za-z][A-Za-z'’]+)"
    pats = [rf"\b{word}(?:\s+[A-Za-z]\.?)?\s+{ln}\b", rf"\b{ln}\s*,\s*{word}\b"]
    found = []
    for pat in pats:
        for m in re.finditer(pat, flat, re.I):
            f = re.sub(r"['’]", "", m.group(1)).lower()
            if len(f) >= 2 and f not in _NOT_FIRST_NAMES and f not in _NAME_SUFFIXES and f not in found:
                found.append(f)
    return found

def _names_signed(text):
    """(first, [last]) pairs from 'signed by First Last' / 'Name: First Last' / 'By: First Last'.
    Used when the requested name has no last name to anchor on."""
    out = []
    for m in re.finditer(r"(?i:signed\s+by|name\s*:|by\s*:)\s*([A-Z][A-Za-z'’]+)\s+(?:[A-Z]\.?\s+)?([A-Z][A-Za-z'’-]+)",
                         text or ""):
        first, last = _split_person_name(f"{m.group(1)} {m.group(2)}")
        if first and last and first not in _NOT_FIRST_NAMES and (first, last) not in out:
            out.append((first, last))
    return out

def _display_part(original, tokens):
    """The words of `original` that normalize to `tokens` (keeps 'McDonald' casing), else title case."""
    words = [w for w in re.split(r"[\s,]+", original or "") if w]
    n = len(tokens)
    for i in range(len(words) - n + 1):
        if [t for w in words[i:i + n] for t in _name_tokens(w)] == tokens:
            return " ".join(words[i:i + n])
    return " ".join(tokens).title()

def _pick_identity(requested, clues, nick):
    """Strongest clue consistent with the requested name. clues: ordered list of
    {"source", "detail", "first", "last" (token list), "emails"}. A clue counts only if it names the
    same person: same last name and a consistent first name (or, when only one name was requested,
    either part matches). Within a source tier the longest first name wins (Zachary over Zach).
    Returns the chosen clue dict, or a synthesized one from the requested name."""
    rf, rl = _split_person_name(requested)

    def consistent(c):
        if not c.get("first") or not c.get("last"):
            return False
        if rf:
            return c["last"] == rl and _first_names_consistent(rf, c["first"], nick)
        return c["last"] == rl or _first_names_consistent(rl[0] if rl else "", c["first"], nick)

    tiers = {}
    for c in clues:
        if consistent(c):
            tiers.setdefault(c["source"], []).append(c)
    for source in ("email", "document name", "email body"):
        if tiers.get(source):
            if source == "email":
                return tiers[source][0]
            return max(tiers[source], key=lambda c: len(c["first"]))
    return {"source": "requested name", "detail": requested, "first": rf, "last": rl, "emails": []}

def _person_folder_candidates(folders, last):
    """Folders whose title starts with the last name ('Last, First'), plus legacy 'First Last' titles
    ending with it. Returns [(folder, folder_first_name_or_empty)]."""
    out = []
    n = len(last)
    for f in folders:
        toks = _name_tokens(f.get("name"))
        words = [t for t in toks if t != ","]
        if words[:n] == last:
            rest = words[n:]
            out.append((f, rest[0] if rest else ""))
        elif "," not in toks and len(words) > n and words[-n:] == last:
            out.append((f, words[0]))
    return out

def _match_person_folders(first, last, folders, nick, contents_match=None):
    """Spec'd folder rule. Returns [(folder, reason)] where reason is 'first name', 'nickname' or
    'folder contents'. contents_match(list_of_folders) -> set of folder ids whose files mention the
    person (by email or full name); only consulted for candidates not matched by name."""
    cands = _person_folder_candidates(folders, last)
    matches, rest = [], []
    for f, ff in cands:
        rel = _first_name_relation(first, ff, nick)
        if rel:
            matches.append((f, rel))
        else:
            rest.append(f)
    if rest and contents_match:
        hit = contents_match(rest) or set()
        matches += [(f, "folder contents") for f in rest if f["id"] in hit]
    return matches

def _pdf_text(data):
    if PdfReader is None or not data:
        return ""
    try:
        import io
        reader = PdfReader(io.BytesIO(data))
        out = []
        for page in reader.pages[:_PDF_MAX_PAGES]:
            out.append(page.extract_text() or "")
            if sum(len(x) for x in out) > _PDF_MAX_CHARS:
                break
        return "\n".join(out)[:_PDF_MAX_CHARS]
    except Exception as e:
        logger.warning(f"PDF text extraction failed: {e}")
        return ""

def _attachment_text(a):
    if "_text" not in a:
        mime = (a.get("mime") or "").lower()
        if mime == "application/pdf" or a.get("filename", "").lower().endswith(".pdf"):
            a["_text"] = _pdf_text(a.get("data"))
        elif mime.startswith("text/"):
            a["_text"] = (a.get("data") or b"").decode("utf-8", errors="replace")[:_PDF_MAX_CHARS]
        else:
            a["_text"] = ""
    return a["_text"]

def _lookup_person_by_email(email):
    for p in get_snapshot("people.json"):
        if email in {(p.get(k) or "").lower().strip() for k in ("email", "email2", "home_email")}:
            return p
    r = call_pipeline_api("GET", f"/people.json?conditions[email]={urllib.parse.quote(email)}&per_page=3")
    if r["status"] == 200 and isinstance(r["data"], dict):
        for p in r["data"].get("entries", []):
            if (p.get("email") or "").lower().strip() == email:
                return p
    return None

def _gather_identity_clues(requested, attachments):
    """Ordered clues: email (sender, forwarded sender, attachment text → Pipeline person),
    document name (attachment file names and PDF text), email body (subject + body)."""
    em = _CURRENT_EMAIL or {}
    body = em.get("body") or ""
    texts = [(a.get("filename", ""), _attachment_text(a)) for a in (attachments or [])]
    rf, rl = _split_person_name(requested)

    sender = [em.get("from") or ""] + re.findall(r"^[>\s]*From:[^\n]*", body, re.M | re.I)
    emails = []
    for e in _extract_emails(" ".join(sender)) + [e for _, t in texts for e in _extract_emails(t)]:
        if not _is_internal_email(e) and e not in emails:
            emails.append(e)
    clues, seen_ids = [], set()
    for e in emails[:15]:
        p = _lookup_person_by_email(e)
        if not p or p.get("id") in seen_ids:
            continue
        seen_ids.add(p.get("id"))
        pf, pl = _split_person_name(f"{p.get('first_name') or ''} {p.get('last_name') or ''}")
        p_emails = [x for x in ((p.get(k) or "").lower().strip() for k in ("email", "email2", "home_email")) if x]
        clues.append({"source": "email", "detail": f"{e} → Pipeline person {p.get('id')}",
                      "first": pf, "last": pl, "emails": p_emails or [e], "person_id": p.get("id"),
                      "display_first": (p.get("first_name") or "").strip(),
                      "display_last": (p.get("last_name") or "").strip()})

    def name_clues(source, label, text, last):
        out = []
        if last and rf:   # anchored search only with a first name to check it against
            out += [{"source": source, "detail": label, "first": f, "last": last, "emails": []}
                    for f in _names_with_last(text, last)]
        out += [{"source": source, "detail": label, "first": f, "last": l, "emails": []}
                for f, l in _names_signed(text)]
        return out

    for fname, text in texts:
        stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", fname)
        clues += name_clues("document name", f"file name '{fname}'", re.sub(r"[-_]+", " ", stem), rl)
        clues += name_clues("document name", f"text of '{fname}'", text, rl)
    clues += name_clues("email body", "email subject/body", f"{em.get('subject') or ''}\n{body}", rl)
    return clues

def _escape_drive_q(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")

def _folders_mentioning(folders, terms):
    """Ids of folders (among `folders`) holding a file whose title or text contains any of terms."""
    terms = [t for t in terms if t]
    if not folders or not terms:
        return set()
    parents = " or ".join(f"'{_escape_drive_q(f['id'])}' in parents" for f in folders)
    text = " or ".join(f"fullText contains '\"{_escape_drive_q(t)}\"'" for t in terms)
    files, err = _drive_list(f"trashed=false and ({parents}) and ({text})",
                             fields="nextPageToken,files(id,name,parents)")
    if err:
        logger.warning(f"Folder contents search failed: {err}")
        return set()
    ids = {f["id"] for f in folders}
    return {p for f in files for p in f.get("parents", []) if p in ids}

def _resolve_person_folder(client_name, attachments, create_if_missing, create_new, close_match_review=None):
    nick = _nickname_map()
    try:
        clues = _gather_identity_clues(client_name, attachments)
    except Exception as e:
        logger.warning(f"Identity clue gathering failed: {e}")
        clues = []
    ident = _pick_identity(client_name, clues, nick)
    first, last = ident.get("first"), ident.get("last")
    identity = {"source": ident["source"], "detail": ident.get("detail"),
                "first_name": ident.get("display_first") or _display_part(client_name, [first] if first else []),
                "last_name": ident.get("display_last") or _display_part(client_name, last or [])}
    logger.info(f"Person folder identity for '{client_name}': {json.dumps(identity)}")
    if not last:
        return {"status": "error", "http_status": None, "identity": identity,
                "body": "Could not determine the client's last name; ask Chad for the full name."}
    folders, err = _list_client_folders()
    if err:
        return {"status": "error", "http_status": err["status"], "body": err["data"]}

    def folder_entry(f, reason):
        e = {k: v for k, v in _folder_result("", f).items() if k != "status"}
        e["matched_by"] = reason
        return e

    if not first:
        cands = [f for f, _ in _person_folder_candidates(folders, last)]
        if cands:
            return {"status": "ambiguous", "reason": "first_name_unknown", "identity": identity,
                    "folders": [folder_entry(f, "last name only") for f in cands]}
        return {"status": "error", "http_status": None, "identity": identity,
                "body": "Could not determine the client's first name; ask Chad for the full name."}

    full = f"{identity['first_name']} {identity['last_name']}"
    terms = list(dict.fromkeys(ident.get("emails", []) + [full, f"{identity['last_name']}, {identity['first_name']}"]))
    matches = _match_person_folders(first, last, folders, nick, lambda fs: _folders_mentioning(fs, terms))
    if len(matches) > 1 and not create_new:
        return {"status": "ambiguous", "reason": "multiple_person_folders", "identity": identity,
                "folders": [folder_entry(f, r) for f, r in matches]}
    if len(matches) == 1:
        f, reason = matches[0]
        out = _folder_result("exists", f)
        out.update({"matched_by": reason, "identity": identity})
        return out
    close = _close_person_folders(first, last, folders, nick, {f["id"] for f, _ in matches})
    if not create_if_missing and not create_new:
        return {"status": "not_found", "searched_name": full, "identity": identity,
                "close_folders": _close_entries(close)}
    new_name = f"{identity['last_name']}, {identity['first_name']}"
    return _create_client_folder(new_name, close, close_match_review, create_new,
                                 {"matched_by": "new folder", "identity": identity})

# ── Close-name check (runs before any new client folder is created) ──────────
# Like RULE 13's company duplicate ladder: a new folder is only created once the agent has reviewed
# every close-named folder (or Chad said to create new), and the creation is always disclosed.
_ENTITY_DESCRIPTORS = {"holdings", "group", "technologies", "technology", "tech", "labs", "systems",
                       "robotics", "dynamics", "industries", "networks", "computing", "sciences", "bio",
                       "health", "space", "energy", "motors", "software", "solutions", "ventures",
                       "partners", "capital", "ai", "fund", "trust", "family", "the", "and", "of"}
_GENERIC_FIRST_WORDS = {"new", "global", "american", "first", "united", "international", "national",
                        "the", "north", "south", "east", "west", "general", "great"}
_CLOSE_RATIO = 0.8

def _similar(a, b, ratio=_CLOSE_RATIO):
    return bool(a) and bool(b) and difflib.SequenceMatcher(None, a, b).ratio() >= ratio

def _parse_person_folder(name):
    """(first, [last...], words) for 'Last, First ...' or 'First ... Last' folder titles."""
    toks = [t for t in _name_tokens(name) if t not in _NAME_SUFFIXES]
    words = [t for t in toks if t != ","]
    if "," in toks:
        i = toks.index(",")
        last = [t for t in toks[:i] if t != ","]
        after = [t for t in toks[i + 1:] if t != ","]
        return (after[0] if after else ""), last, words
    if len(words) >= 2:
        return words[0], words[-1:], words
    return "", words, words

def _close_person_folders(first, last, folders, nick, exclude_ids=()):
    """Folders that might be the same person but did not match outright. Returns [(folder, why)]."""
    out = []
    lj = " ".join(last)
    for f in folders:
        if f["id"] in exclude_ids:
            continue
        ff, fl, words = _parse_person_folder(f.get("name"))
        first_ok = (not ff) or _first_names_consistent(first, ff, nick) or ff[:1] == (first or "")[:1]
        if fl == last:
            why = "same last name"
        elif all(t in words for t in last):
            why = "contains the last name"
        elif _similar(" ".join(fl), lj) and first_ok:
            why = "similar last name spelling"
        elif ff and _first_names_consistent(first, ff, nick) and any(_similar(w, lj) for w in words):
            why = "same first name, similar last name"
        else:
            continue
        out.append((f, why))
    return out

def _entity_root(name):
    toks = _folder_tokens(name, drop_suffixes=False)
    root = [t for t in toks if t not in _ENTITY_SUFFIXES and t not in _ENTITY_DESCRIPTORS]
    return root or toks

def _close_entity_folders(name, folders, near=(), exclude_ids=()):
    """Folders that might be the same entity under another name form. Returns [(folder, why)]."""
    near_ids = {f["id"] for f in near}
    root = _entity_root(name)
    flat = " ".join(_folder_tokens(name))
    out = []
    for f in folders:
        if f["id"] in exclude_ids or not _folder_tokens(f.get("name")):
            continue
        froot = _entity_root(f.get("name"))
        n = min(len(root), len(froot))
        if f["id"] in near_ids:
            why = "similar name"
        elif n and root[:n] == froot[:n]:
            why = "same root name"
        elif root and froot and root[0] == froot[0] and len(root[0]) >= 4 and root[0] not in _GENERIC_FIRST_WORDS:
            why = "same first word"
        elif _similar(" ".join(_folder_tokens(f.get("name"))), flat, 0.85):
            why = "similar spelling"
        else:
            continue
        out.append((f, why))
    return out

def _close_entries(close):
    return [{**{k: v for k, v in _folder_result("", f).items() if k != "status"}, "why_close": why}
            for f, why in close]

def _create_client_folder(new_name, close, close_match_review, create_new, extra=None):
    """Creates the folder unless close-named folders exist and nobody reviewed them. Every result that
    creates carries close_name_check and a disclosure line the agent must put in its reply."""
    extra = dict(extra or {})
    review = (close_match_review or "").strip()
    if close and not review and not create_new:
        return {"status": "close_matches", "proposed_name": new_name, "folders": _close_entries(close),
                "message": "Nothing created. Review each folder: reuse it (folder_id) if it is the same client, "
                           "ask Chad if unsure, or call again with close_match_review if every one is a "
                           "different client.", **extra}
    r = _drive_request("POST", DRIVE_FILES_URL, params={"fields": "id,name"}, payload={
        "name": new_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [DRIVE_CLIENTS_FOLDER_ID],
    })
    if r["status"] != 200 or not isinstance(r["data"], dict):
        return {"status": "error", "http_status": r["status"], "body": r["data"]}
    out = _folder_result("created", r["data"])
    if not close:
        check = "no close names found"
    elif review:
        check = f"compared against {', '.join(f['name'] for f, _ in close)} — {review}"
    else:
        check = f"close names {', '.join(f['name'] for f, _ in close)} — Chad asked to create a new folder"
    out.update(extra)
    out["close_name_check"] = {"compared_against": _close_entries(close), "review": review or None,
                               "created_on_chads_instruction": bool(create_new and not review)}
    out["disclosure"] = f"NEW FOLDER CREATED: {out['name']} — close-name check: {check}"
    return out

def _chosen_client_folder(folder_id):
    """Folder Chad picked by id (after an ambiguous result). Must be a folder directly under CLIENTS."""
    r = _drive_request("GET", f"{DRIVE_FILES_URL}/{folder_id}", params={"fields": "id,name,mimeType,parents,trashed"})
    if r["status"] != 200 or not isinstance(r["data"], dict):
        return {"status": "error", "http_status": r["status"], "body": r["data"]}
    d = r["data"]
    if d.get("mimeType") != "application/vnd.google-apps.folder" or d.get("trashed") \
            or DRIVE_CLIENTS_FOLDER_ID not in (d.get("parents") or []):
        return {"status": "error", "http_status": None, "body": f"{folder_id} is not a client folder under CLIENTS"}
    out = _folder_result("exists", d)
    out["matched_by"] = "folder chosen by Chad"
    return out

def ensure_client_folder(client_name, client_type, create_if_missing=True, create_new=False,
                         folder_id=None, attachments=None, close_match_review=None):
    if folder_id:
        return _chosen_client_folder(str(folder_id).strip())
    client_name = " ".join((client_name or "").split())
    if not client_name:
        return {"status": "error", "http_status": None, "body": "client_name is required"}
    if client_type not in ("entity", "person"):
        return {"status": "error", "http_status": None, "body": "client_type must be 'entity' or 'person'"}
    if client_type == "person":
        return _resolve_person_folder(client_name, attachments, create_if_missing, create_new, close_match_review)
    folders, err = _list_client_folders()
    if err:
        return {"status": "error", "http_status": err["status"], "body": err["data"]}
    matches, near = _match_client_folders(client_name, client_type, folders)
    if len(matches) > 1:
        return {"status": "ambiguous",
                "folders": [{k: v for k, v in _folder_result("", f).items() if k != "status"} for f in matches]}
    if matches:
        return _folder_result("exists", matches[0])
    # Similar-but-not-equal names (e.g. "Fund II LLC" vs "Fund"): never auto-pick; reviewed before creating.
    close = _close_entity_folders(client_name, folders, near)
    if not create_if_missing and not create_new:
        return {"status": "not_found", "searched_name": client_name, "close_folders": _close_entries(close)}
    return _create_client_folder(client_name, close, close_match_review, create_new)

# ── Drive file upload / rename ────────────────────────────────────────────────
DRIVE_UPLOAD_URL         = "https://www.googleapis.com/upload/drive/v3/files"
DRIVE_MULTIPART_MAX      = 5 * 1024 * 1024

# Attachments of the email being processed. Reset at the start of every invocation;
# bytes are only reachable by tools, never sent to the model.
_CURRENT_ATTACHMENTS = []

def _clean_filename(name):
    """Collapse whitespace runs (incl. CR/LF/tab left by folded MIME headers) to one space."""
    return " ".join((name or "").split())

def _fmt_size(n):
    return f"{max(1, round(n / 1024))} KB" if n < 1024 * 1024 else f"{n / (1024 * 1024):.1f} MB"

def _file_url(file_id):
    return f"https://drive.google.com/file/d/{file_id}/view"

def _split_ext(name):
    stem, dot, ext = name.rpartition(".")
    if not dot or not stem or " " in ext or len(ext) > 10:
        return name, ""
    return stem, "." + ext

def _unique_name(name, taken):
    if name not in taken:
        return name, False
    stem, ext = _split_ext(name)
    n = 2
    while f"{stem} ({n}){ext}" in taken:
        n += 1
    return f"{stem} ({n}){ext}", True

def _drive_upload(folder_id, name, mime, data):
    """Upload bytes into folder_id. Returns {"status", "data"}; data has id,name on success."""
    meta = json.dumps({"name": name, "parents": [folder_id]}).encode()
    if len(data) <= DRIVE_MULTIPART_MAX:
        boundary = "pipelineagent" + os.urandom(12).hex()
        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode() + meta + \
               (f"\r\n--{boundary}\r\nContent-Type: {mime}\r\n\r\n").encode() + data + \
               (f"\r\n--{boundary}--\r\n").encode()
        return _drive_raw("POST", DRIVE_UPLOAD_URL,
                          params={"uploadType": "multipart", "fields": "id,name"}, body=body,
                          headers={"Content-Type": f"multipart/related; boundary={boundary}"})
    start = _drive_raw("POST", DRIVE_UPLOAD_URL,
                       params={"uploadType": "resumable", "fields": "id,name"}, body=meta,
                       headers={"Content-Type": "application/json; charset=UTF-8",
                                "X-Upload-Content-Type": mime,
                                "X-Upload-Content-Length": str(len(data))})
    session_url = {k.lower(): v for k, v in start.get("headers", {}).items()}.get("location")
    if start["status"] != 200 or not session_url:
        return start
    return _drive_raw("PUT", session_url, body=data,
                      headers={"Content-Type": mime, "Content-Length": str(len(data))})

def upload_to_client_folder(client_name, client_type, attachment_indexes=None, create_if_missing=True,
                            create_new=False, folder_id=None, close_match_review=None):
    atts = list(_CURRENT_ATTACHMENTS)
    if not atts:
        return {"status": "no_attachments", "message": "This email has no attachments to file."}
    errors = []
    if attachment_indexes:
        by_index = {a["index"]: a for a in atts}
        selected = []
        for i in attachment_indexes:
            try:
                i = int(i)
            except (TypeError, ValueError):
                errors.append({"index": i, "error": "invalid attachment index"})
                continue
            if i in by_index:
                selected.append(by_index[i])
            else:
                errors.append({"index": i, "error": f"no attachment #{i} (email has {len(atts)})"})
    else:
        selected = atts
    folder = ensure_client_folder(client_name, client_type, create_if_missing, create_new,
                                  folder_id=folder_id, attachments=selected or atts,
                                  close_match_review=close_match_review)
    if folder.get("status") not in ("exists", "created"):
        return folder
    existing, err = _list_folder_files(folder["folder_id"])
    if err:
        return {"status": "error", "http_status": err["status"], "body": err["data"], "folder": folder}
    taken = {f["name"] for f in existing}
    files = []
    for a in selected:
        name, renamed = _unique_name(a["filename"], taken)
        r = _drive_upload(folder["folder_id"], name, a["mime"], a["data"])
        if r["status"] in (200, 201) and isinstance(r["data"], dict) and r["data"].get("id"):
            taken.add(r["data"].get("name", name))
            files.append({"index": a["index"], "name": r["data"].get("name", name), "file_id": r["data"]["id"],
                          "url": _file_url(r["data"]["id"]), "renamed_due_to_duplicate": renamed})
        else:
            errors.append({"index": a["index"], "name": a["filename"], "http_status": r["status"], "body": r["data"]})
    return {"status": "uploaded" if files else "error", "folder": folder, "files": files, "errors": errors}

def rename_drive_file(new_name, file_id=None, current_name=None, client_name=None, client_type=None):
    new_name = _clean_filename(new_name)
    if not new_name:
        return {"status": "error", "http_status": None, "body": "new_name is required"}
    if file_id:
        r = _drive_request("GET", f"{DRIVE_FILES_URL}/{file_id}", params={"fields": "id,name"})
        if r["status"] != 200 or not isinstance(r["data"], dict):
            return {"status": "error", "http_status": r["status"], "body": r["data"]}
        target = r["data"]
    else:
        if not current_name or not client_name or not client_type:
            return {"status": "error", "http_status": None,
                    "body": "Provide file_id, or current_name with client_name and client_type"}
        folder = ensure_client_folder(client_name, client_type, create_if_missing=False)
        if folder.get("status") != "exists":
            return folder
        files, err = _list_folder_files(folder["folder_id"])
        if err:
            return {"status": "error", "http_status": err["status"], "body": err["data"], "folder": folder}
        q = current_name.strip().lower()
        matches = [f for f in files if f["name"].lower() == q] or [f for f in files if q in f["name"].lower()]
        if not matches:
            return {"status": "not_found", "current_name": current_name, "folder": folder}
        if len(matches) > 1:
            return {"status": "ambiguous", "folder": folder,
                    "files": [{"file_id": f["id"], "name": f["name"], "url": _file_url(f["id"])} for f in matches]}
        target = matches[0]
    old_name = target["name"]
    old_ext = _split_ext(old_name)[1]
    if old_ext and not _split_ext(new_name)[1]:
        new_name += old_ext
    r = _drive_request("PATCH", f"{DRIVE_FILES_URL}/{target['id']}", params={"fields": "id,name"},
                       payload={"name": new_name})
    if r["status"] != 200 or not isinstance(r["data"], dict):
        return {"status": "error", "http_status": r["status"], "body": r["data"]}
    return {"status": "renamed", "file_id": target["id"], "old_name": old_name,
            "new_name": r["data"].get("name", new_name), "url": _file_url(target["id"])}

# ── Tool definitions ──────────────────────────────────────────────────────────
TOOL_SPECS = [
    {
        "toolSpec": {
            "name": "search_companies",
            "description": "Search Pipeline CRM companies by name using conditions[company_name]. Use this to find a specific company record.",
            "inputSchema": {"json": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}}
        }
    },
    {
        "toolSpec": {
            "name": "get_company",
            "description": "Get a company record by ID including all custom fields.",
            "inputSchema": {"json": {"type": "object", "properties": {"company_id": {"type": "integer"}}, "required": ["company_id"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_people",
            "description": "Search Pipeline CRM contacts by name or email using conditions[person_name].",
            "inputSchema": {"json": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}
        }
    },
    {
        "toolSpec": {
            "name": "get_person",
            "description": "Get a contact record by ID.",
            "inputSchema": {"json": {"type": "object", "properties": {"person_id": {"type": "integer"}}, "required": ["person_id"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_deals",
            "description": "Search deals. Filter by company_id, person_id, name, or status. The status field is the color badge only (1=Red,2=Yellow,3=Green) — do NOT use it to represent deal stage. To filter by stage, use deal_stage_id.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "company_id": {"type": "integer"},
                "person_id": {"type": "integer"},
                "name": {"type": "string"},
                "status": {"type": "integer"},
                "per_page": {"type": "integer"}
            }}}
        }
    },
    {
        "toolSpec": {
            "name": "get_deal",
            "description": "Get a deal record by ID.",
            "inputSchema": {"json": {"type": "object", "properties": {"deal_id": {"type": "integer"}}, "required": ["deal_id"]}}
        }
    },
    {
        "toolSpec": {
            "name": "create_person",
            "description": "Create a new contact record. Always search by email first to confirm they don't already exist before creating.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "first_name": {"type": "string"},
                "last_name": {"type": "string"},
                "email": {"type": "string"},
                "phone": {"type": "string", "description": "Pass exactly as written on the source; the tool normalizes to E.164 and refuses (flags) any number that would lose digits."},
                "mobile": {"type": "string"},
                "phone_country": {"type": "string", "description": "Country the phone belongs to (e.g. the form's country). Hint for numbers written without +/00; not stored."},
                "company_name": {"type": "string"},
                "position": {"type": "string"},
                "linked_in_url": {"type": "string"},
                "website": {"type": "string"},
                "summary": {"type": "string"},
                "work_city": {"type": "string"},
                "work_country": {"type": "string"},
                "custom_label_3075382": {"type": "integer", "description": "Role: 6596061=Investor, 6438705=Intermediary"},
                "custom_label_3759163": {"type": "integer", "description": "Transactor Type: 6484811=Family Office, 6484810=Natural Person, 6484812=Institution, 6484808=VC or PE Fund, 7037492=Hedge Fund, 6484813=Wealth Advisor, 6577160=Co-Broker, 6888332=Foreign Finder"},
                "custom_label_3923758": {"type": "integer", "description": "Investor Level: 7162165=Substantive (default), 6950564=Qualified Purchaser, 7209227=Qualified Client, 6950563=Accredited Investor, 6950562=Non-Accredited, 7161646=Hold: Screen for Substantive, 6950561=Unknown"},
                "custom_label_3775335": {"type": "integer", "description": "Weekly Newsletter: 6613674=Subscribed, 6613673=Confirm, 6609023=Unsubscribed, 6546189=Hold, 6536755=No, 6546212=Screen, 6795370=No - Intermediary, 6795371=No - Colleague Point Person. Use No - Intermediary when Role = Intermediary."}
            }, "required": ["first_name", "last_name"]}}
        }
    },
    {
        "toolSpec": {
            "name": "update_person",
            "description": "Update a contact record fields.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "person_id": {"type": "integer"},
                "fields": {"type": "object"}
            }, "required": ["person_id", "fields"]}}
        }
    },
    {
        "toolSpec": {
            "name": "update_company",
            "description": "Update a company record fields. Hiive market data fields: hiive_bid (custom_label_3997298) = highest bid price on Hiive (currency, per share), hiive_ask (custom_label_3997297) = lowest ask price on Hiive (currency, per share). The corresponding date fields (custom_label_3997299 ask date, custom_label_3997300 bid date) are set automatically to today when bid or ask is updated — never pass them explicitly.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "company_id": {"type": "integer"},
                "fields": {"type": "object", "description": "Fields to update. Address: address_1 (street), address_2, city, state, postal_code, country — these overwrite the company's current address. Phone: phone1 (pass as written; normalized to E.164, refused if digits would be lost); optional phone_country hint (not stored). Includes Hiive market data: custom_label_3997298=hiive_bid (highest bid, per share), custom_label_3997297=hiive_ask (lowest ask, per share). Bid/ask date fields are auto-set to today — never pass them explicitly."},
                "industry": {"type": "array", "items": {"type": "string"}, "description": "List of industry tags e.g. [\"AI\", \"Defense\", \"Robotics\"]. Will be mapped to entry IDs automatically and merged with existing values."}
            }, "required": ["company_id", "fields"]}}
        }
    },
    {
        "toolSpec": {
            "name": "create_company",
            "description": "Create a new company record in Pipeline CRM.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "name": {"type": "string"},
                "summary": {"type": "string"},
                "org_type": {"type": "integer", "description": "Org type entry ID e.g. 6677589=Private Company, 5103523=Unicorn, 6298036=Public Company, 4497856=GP:VC, 444501=GP:PE, 444492=LP:Family Office"},
                "last_round_pps": {"type": "number", "description": "Last round price per share"},
                "last_round_valuation": {"type": "number", "description": "Last round valuation in billions (e.g. $7.6B = 7.6, $18B = 18.0)"},
                "last_round_date": {"type": "string", "description": "Last round date in YYYY-MM-DD format"},
                "last_round_series": {"type": "string", "description": "Last round series e.g. A, B, C, Seed"},
                "website": {"type": "string", "description": "Company website"},
                "address": {"type": "string", "description": "Company address"},
                "legal_name": {"type": "string", "description": "Full legal name"},
                "industry": {"type": "array", "items": {"type": "string"}, "description": "List of industry tags e.g. [\"AI\", \"Defense\", \"Robotics\"]. Will be mapped to entry IDs automatically."}
            }, "required": ["name"]}}
        }
    },
    {
        "toolSpec": {
            "name": "add_note",
            "description": "Add a note to a person, company, or deal record. note_category_id options: 69759=Email, 69774=Phone Call, 69758=In Person Meeting, 69760=Background, 69761=Biographical, 69773=Sensitive. Default is Email (69759) if not specified.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "note": {"type": "string"},
                "person_id": {"type": "integer"},
                "company_id": {"type": "integer"},
                "deal_id": {"type": "integer"}
            }, "required": ["note"]}}
        }
    },
    {
        "toolSpec": {
            "name": "get_security_ids",
            "description": "REQUIRED: Call this before updating any buying/selling/holding field, or before searching by security interest. Returns the correct entry IDs for a security across Holding, Buy, and Sell fields. NEVER use a company Pipeline record ID as a security entry ID — always use this tool.",
            "inputSchema": {"json": {"type": "object", "properties": {"security_name": {"type": "string", "description": "Single security name e.g. Anthropic"}, "security_names": {"type": "array", "items": {"type": "string"}, "description": "Array of names for bulk lookup e.g. [\"Anthropic\", \"SpaceX\"]"}}}}
        }
    },
    {
        "toolSpec": {
            "name": "add_security",
            "description": "Add a NEW security as a dropdown entry in the person Buy, Sell, and Holding interest fields. ONLY call this when Chad explicitly instructs you to add a new tracked security. NEVER call it automatically while processing emails or forms. Checks for near-duplicate names first and refuses if one exists.",
            "inputSchema": {"json": {"type": "object", "properties": {"security_name": {"type": "string", "description": "Exact security name to create, e.g. 'Moonshot AI'"}, "force": {"type": "boolean", "description": "Bypass the near-duplicate guard. Set true ONLY when Chad has explicitly confirmed the name is a different security (e.g. said 'override duplicate check'). Always disclose the bypassed match in your reply."}}, "required": ["security_name"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_people_by_interest",
            "description": "Find people who hold, want to buy, or want to sell a specific security. Use get_security_ids first to get the entry_id.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "entry_id": {"type": "integer"},
                "field_type": {"type": "string", "description": "One of: holding, buy, sell"},
                "per_page": {"type": "integer"}
            }, "required": ["entry_id", "field_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_companies_by_interest",
            "description": "Find companies who hold, want to buy, or want to sell a specific security. Use get_security_ids first to get the entry_id.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "entry_id": {"type": "integer"},
                "field_type": {"type": "string", "description": "One of: holding, buy, sell"},
                "per_page": {"type": "integer"}
            }, "required": ["entry_id", "field_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "find_investors",
            "description": "Find, filter, and rank potential investors in a single call — across one or multiple securities. Use this for ALL investor matching instead of search_people_by_interest + get_person. For a known company use security_name. For comparable-company searches (unknown company), pass security_names as an array of comparables — results are merged, deduplicated, and ranked in one pass. Handles all filtering (priority, role, ticket size, whitelist, red-flag summaries) internally.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "security_name": {"type": "string", "description": "Single security name e.g. 'Anthropic'. Use this OR security_names, not both."},
                "security_names": {"type": "array", "items": {"type": "string"}, "description": "Array of security names for comparable-company search e.g. ['Figure AI', 'Agility Robotics', '1X']. Results merged and deduplicated."},
                "interest_type": {"type": "string", "description": "buy or sell — determines which interest field to search"},
                "min_ticket": {"type": "number", "description": "Minimum deal size in dollars — filters out investors whose max ticket is below this"},
                "limit": {"type": "integer", "description": "Number of results to return (default 25)"},
                "red_flag_terms": {"type": "array", "items": {"type": "string"}, "description": "Additional summary red-flag terms beyond the defaults"}
            }, "required": ["interest_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "compare_pdf_contacts",
            "description": "Compare names and emails extracted from a PDF against the CRM snapshot to find who is already in Pipeline and who is not. Use this when Chad attaches a PDF (e.g. PitchBook investor list) and asks which people are or aren't already in his CRM. Pass the raw text from the PDF and optionally a security_name to also check holding/buy/sell interest. All matching is done in Python — no need to call get_person or search_people separately.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "pdf_text": {"type": "string", "description": "Raw text extracted from the PDF containing names, emails, or company names to cross-reference"},
                "security_name": {"type": "string", "description": "Optional: security name (e.g. 'Anthropic') — if provided, also checks whether matched CRM contacts already hold/buy/sell this security"},
                "interest_type": {"type": "string", "description": "Optional: holding, buy, or sell — used with security_name to check interest field"}
            }, "required": ["pdf_text"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_deals_cache",
            "description": "Search the deals cache to find buy or sell opportunities matching specific criteria. Use this for deal discovery queries like 'what deals would suit this investor', 'show me highlighted SpaceX deals', 'find sell-side deals under $1M'. Returns highlighted deals first. Supports filtering by company name, deal type (buy/sell), highlighted status, country, min/max size, structure, and series.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "company": {"type": "string", "description": "Company name to search for (partial match)"},
                "deal_type": {"type": "string", "description": "buy or sell"},
                "highlighted_only": {"type": "boolean", "description": "If true, return only highlighted deals"},
                "country": {"type": "string", "description": "Filter by company country (partial match)"},
                "max_size": {"type": "number", "description": "Maximum deal size filter in dollars"},
                "min_size": {"type": "number", "description": "Minimum deal size filter in dollars"},
                "structure": {"type": "string", "description": "Deal structure: Direct, Fund, or Forward"},
                "series": {"type": "string", "description": "Deal series e.g. A, B, C, D, E, F, G"},
                "limit": {"type": "integer", "description": "Max results to return (default 20)"}
            }}}
        }
    },
    {
        "toolSpec": {
            "name": "web_search",
            "description": "Search the web using DuckDuckGo. Use this to research a person or company — find their location, what they do, fund type, AUM, background, etc. Returns a list of result snippets and URLs. Call this before fetch_webpage to find the right URLs to visit.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "query": {"type": "string", "description": "Search query, e.g. 'Black Hill Capital Partners LLC investment manager'"}
            }, "required": ["query"]}}
        }
    },
    {
        "toolSpec": {
            "name": "fetch_webpage",
            "description": "Fetch the text content of a webpage URL. Use this to research a company by visiting their website (e.g. https://domain.com). Useful for classifying Transactor Type, understanding what a company does, and writing accurate summaries. Always try the company homepage first.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "url": {"type": "string", "description": "Full URL to fetch, e.g. https://blackhill-capital.com"}
            }, "required": ["url"]}}
        }
    },
    {
        "toolSpec": {
            "name": "create_deal",
            "description": "Create a new deal in Pipeline CRM. Always look up company_id via search_companies and primary_contact_id via search_people first. Deal name format: 'Company: $MIN - $MAX Buy/Sell' (min=max → 'Company: $X Sell'; min only → 'Company: $MIN+ Buy'; max only → 'Company: up to $MAX Sell'; no size → 'Company: Buy'). K/M/B amounts, drop trailing .0, spaced hyphen not en dash. Revenue type is always Commission. Stage is Firm only if BOTH a price field (gross or net) AND a size field (min or max) are present; Inquiry otherwise. Messaging is always set to Disallow on creation.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "name": {"type": "string", "description": "Deal name, company first then colon, e.g. 'Anthropic: $100K - $250K Buy'"},
                "company_id": {"type": "integer", "description": "Pipeline company ID — look up via search_companies first"},
                "primary_contact_id": {"type": "integer", "description": "Pipeline person ID — look up via search_people first"},
                "deal_stage_id": {"type": "integer", "description": "2109142=Inquiry, 111800=Firm, 2381534=Matched, 2388323=Confirm, 2094373=Hold. Default Inquiry; use Firm only when BOTH a price field (gross or net) AND a size field (min or max) are present."},
                "deal_type": {"type": "string", "enum": ["buy", "sell"], "description": "buy or sell — required"},
                "gross": {"type": "number", "description": "Gross price per share"},
                "net": {"type": "number", "description": "Net price per share"},
                "min_size": {"type": "number", "description": "Minimum deal size in dollars. 'Min ticket' refers to deal size — map a minimum ticket here."},
                "max_size": {"type": "number", "description": "Maximum deal size in dollars. 'Ticket', 'ticket size', or 'max ticket' refer to deal size — map a maximum ticket here. A single ticket figure with no min/max qualifier (e.g. 'ticket of $5M') is the maximum."},
                "structure": {"type": "array", "items": {"type": "string", "enum": ["Direct", "Fund", "Forward"]}, "description": "Deal structure(s) — multi-select; pass every value that applies, e.g. [\"Direct\", \"Forward\"]"},
                "series": {"type": "string", "description": "Series e.g. A, B, C, D, E, F, Seed, N/A"},
                "class": {"type": "string", "enum": ["Common", "Preferred", "Mixed", "Any"], "description": "Share class"},
                "nexus": {"type": "string", "enum": ["Direct", "RMS Broker", "Co-Broker", "Foreign Finder"], "description": "How the deal came in"},
                "management_fee": {"type": "number", "description": "SPV management fee percentage"},
                "carry": {"type": "number", "description": "SPV carry percentage"},
                "seller_fee": {"type": "number", "description": "One-time upfront seller fee % (not annual). Use for 'upfront fee', 'one-time fee', or 'seller fee' mentions."},
                "partner_fee": {"type": "number", "description": "Fee % charged by a co-broker, foreign finder, or other intermediary that is NOT covered by our standard fee-sharing agreement. Use this when a third party is charging their own separate fee to the client that Chad does not share in."},
                "layers": {"type": "string", "enum": ["SPV on cap table", "2-Layer SPV", "3-Layer SPV"], "description": "SPV layer structure"},
                "num_shares": {"type": "number", "description": "Number of shares in the deal"},
                "refresh": {"type": "number", "description": "Days before stale notice (custom_label_3994687). ALWAYS pass 0 unless Chad explicitly gives another number. 0 = never goes stale."}
            }, "required": ["name", "company_id", "primary_contact_id", "deal_stage_id", "deal_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "update_deal",
            "description": "Update an existing deal in Pipeline CRM. Use get_deal to retrieve current values first if merging data.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "deal_id": {"type": "integer", "description": "Pipeline deal ID"},
                "fields": {"type": "object", "description": "Fields to update — same keys as create_deal plus any custom_label_ fields"}
            }, "required": ["deal_id", "fields"]}}
        }
    },
    {
        "toolSpec": {
            "name": "search_leads_cache",
            "description": "Supplementary cache — may be stale. Prefer search_people_by_interest for accurate live results. NEVER use during an update task.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "security_name": {"type": "string", "description": "Security name e.g. Kraken, Ayar Labs, SpaceX"},
                "interest_type": {"type": "string", "description": "One of: holding, buying, selling"}
            }, "required": ["security_name", "interest_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "ensure_client_folder",
            "description": "Find or create the client's Google Drive folder inside Chad's CLIENTS folder. Only call when Chad explicitly asks to create/find/set up a client or Drive folder. Lists folders live (never cached). Returns status 'exists' or 'created' with folder_id, name, url; 'ambiguous' with a list of matching folders (nothing created) ; 'close_matches' when no folder matches outright but close-named folders exist (nothing created; each has why_close) — review them per the close-name check rule; 'not_found' if create_if_missing is false and no match (with close_folders); 'created' results carry close_name_check and a 'disclosure' line that must appear in the reply; or 'error' with http_status and body. For client_type 'person' the folder is resolved from the strongest identity clue (sender / attachment email addresses looked up in Pipeline, then full names in attachment file names and PDF text, then the email body) and matched by last name plus first name, nickname variant (Zach/Zachary) or folder contents; the result carries 'identity' {source, detail, first_name, last_name} and 'matched_by' ('first name', 'nickname', 'folder contents', 'new folder', 'folder chosen by Chad'); 'ambiguous' with reason 'multiple_person_folders' lists several matching folders, each with matched_by (nothing created).",
            "inputSchema": {"json": {"type": "object", "properties": {
                "client_name": {"type": "string", "description": "Full legal entity name exactly as known (e.g. 'Acme Capital Partners LLC'), or the person's name ('First Last' or 'Last, First')"},
                "client_type": {"type": "string", "enum": ["entity", "person"], "description": "'entity' for a legal entity (folder named exactly as given); 'person' for a natural person (folder named 'Last, First')"},
                "create_if_missing": {"type": "boolean", "description": "Create the folder if no match exists. Default true."},
                "create_new": {"type": "boolean", "description": "Set true ONLY when Chad explicitly said to create a new folder despite close/ambiguous matches. Skips the close-name review (an exact/prefix match is still reused). Default false."},
                "folder_id": {"type": "string", "description": "Drive folder id of an existing client folder: one Chad picked from an 'ambiguous' result, or one you judged to be the same client after a 'close_matches' result. Uses it directly (client_name/client_type are then ignored)."},
                "close_match_review": {"type": "string", "description": "Set ONLY after a 'close_matches' result when every listed folder is clearly a different client: one line per folder, '<folder name>: different client because <evidence>'. The folder is then created and the review is echoed in the disclosure."}
            }, "required": ["client_name", "client_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "upload_to_client_folder",
            "description": "File attachments from the current email into a client's Google Drive folder (resolved like ensure_client_folder; partial entity names match by prefix). Only call when Chad explicitly asks to add/file/save documents. Uploads original bytes and filenames; never overwrites — a name clash is uploaded as 'name (2).ext' and flagged. Returns status 'uploaded' with folder, files [{index, name, file_id, url, renamed_due_to_duplicate}] and errors; or the folder status 'ambiguous' / 'close_matches' / 'not_found' / 'error' (nothing uploaded; 'close_matches' means no folder matched outright but close-named ones exist — review them per the close-name check rule); or 'no_attachments'. If a new folder was created, folder.disclosure must appear in the reply. For client_type 'person' the folder (and the identity) is resolved from the strongest identity clue (sender / attachment email addresses looked up in Pipeline, then full names in attachment file names and PDF text, then the email body) and matched by last name plus first name, nickname variant (Zach/Zachary) or folder contents; the result carries 'identity' {source, detail, first_name, last_name} and 'matched_by' ('first name', 'nickname', 'folder contents', 'new folder', 'folder chosen by Chad'); 'ambiguous' with reason 'multiple_person_folders' lists several matching folders, each with matched_by (nothing created).",
            "inputSchema": {"json": {"type": "object", "properties": {
                "client_name": {"type": "string", "description": "Legal entity name (full or leading part, e.g. 'Preface Ventures'), or the person's name ('First Last' or 'Last, First')"},
                "client_type": {"type": "string", "enum": ["entity", "person"], "description": "'entity' for a company folder; 'person' for a natural person's personal folder"},
                "attachment_indexes": {"type": "array", "items": {"type": "integer"}, "description": "1-based indexes from the email's attachment inventory. Empty or omitted = all attachments."},
                "create_if_missing": {"type": "boolean", "description": "Create the client folder if no match exists. Default true."},
                "create_new": {"type": "boolean", "description": "Set true ONLY when Chad explicitly said to create a new folder despite close/ambiguous matches. Default false."},
                "folder_id": {"type": "string", "description": "Drive folder id of an existing client folder: one Chad picked from an 'ambiguous' result, or one you judged to be the same client after a 'close_matches' result. Uses it directly (client_name/client_type are then ignored)."},
                "close_match_review": {"type": "string", "description": "Set ONLY after a 'close_matches' result when every listed folder is clearly a different client: one line per folder, '<folder name>: different client because <evidence>'. The folder is then created and the review is echoed in the disclosure."}
            }, "required": ["client_name", "client_type"]}}
        }
    },
    {
        "toolSpec": {
            "name": "rename_drive_file",
            "description": "Rename a file in a client's Google Drive folder. Give file_id to rename directly, or client_name + client_type + current_name to find it (case-insensitive: exact match first, then contains). Keeps the original extension if new_name has none. Returns status 'renamed' with file_id, old_name, new_name, url; 'ambiguous' with the matching files; 'not_found'; or 'error' with http_status and body.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "client_name": {"type": "string", "description": "Client folder name (entity or person), required unless file_id is given"},
                "client_type": {"type": "string", "enum": ["entity", "person"], "description": "Required unless file_id is given"},
                "current_name": {"type": "string", "description": "Current file name or a distinctive part of it"},
                "file_id": {"type": "string", "description": "Drive file ID; if given, client_name/current_name are not needed"},
                "new_name": {"type": "string", "description": "New file name"}
            }, "required": ["new_name"]}}
        }
    }
]

# ── Deal naming ───────────────────────────────────────────────────────────────
def _to_amount(v):
    """Parse a size value (number or string like '250,000') to a float; None if blank/invalid."""
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None

def format_deal_amount(amount):
    """K under $1M ($250K), M under $1B ($5M, $1.5M), B from $1B ($1.5B); one decimal, trailing .0 dropped."""
    def _short(x):
        s = f"{x:.1f}"
        return s[:-2] if s.endswith(".0") else s
    for unit, div in (("K", 1_000), ("M", 1_000_000)):
        if amount < div * 1000 and round(amount / div, 1) < 1000:
            return f"${_short(amount / div)}{unit}"
    return f"${_short(amount / 1_000_000_000)}B"

# ── Web offer/bid structure ───────────────────────────────────────────────────
# Forwarded "New web offer/bid" emails (chadgracia/web-bid) carry the submitter's own Structure
# checkboxes. That line — never the person's or company's name or org type — decides Structure.
_WEB_OFFER_STRUCTURES = ("Direct", "Fund", "Forward")

def _parse_web_offer(subject, body):
    """{'side', 'company', 'company_id', 'structure', 'structure_stated'} for a web-bid notification
    (also when forwarded / quoted), else None."""
    text = "\n".join(re.sub(r"^[>\s]+", "", line) for line in (body or "").splitlines())
    subj = re.search(r"New web (offer|bid):\s*(.+?)\s*@", subject or "", re.I) \
        or re.search(r"New web (offer|bid):\s*(.+?)\s*@", text, re.I)
    if not subj and "submitted through the web-bid page" not in text:
        return None
    side = None
    m = re.search(r"firm (sell offer|buy bid) was submitted", text, re.I)
    if m:
        side = "sell" if m.group(1).lower().startswith("sell") else "buy"
    elif subj:
        side = "sell" if subj.group(1).lower() == "offer" else "buy"
    company, company_id = (subj.group(2).strip() if subj else None), None
    m = re.search(r"^Company:\s*(.+?)\s*(?:\(id\s*(\d*)\))?\s*$", text, re.M)
    if m:
        company, company_id = m.group(1).strip(), (m.group(2) or None)
    m = re.search(r"^Structure:\s*(.*)$", text, re.M)
    vals = []
    if m:
        for part in re.split(r"[,/+&]|\band\b", m.group(1)):
            part = part.strip().lower()
            for v in _WEB_OFFER_STRUCTURES:
                if part.startswith(v.lower()) or (v == "Fund" and part == "spv"):
                    if v not in vals:
                        vals.append(v)
    return {"side": side, "company": company, "company_id": company_id,
            "structure": vals, "structure_stated": bool(vals)}

def _offer_structure(offer):
    """(structure list, source label, notice or None) for a parsed web offer/bid."""
    vals = list(offer.get("structure") or [])
    if not vals:
        return ["Direct"], "default, not stated", "Structure not stated, defaulted to Direct, please confirm."
    if "Direct" in vals and "Fund" in vals and offer.get("side") == "sell":
        # One holding can't be both; the form allowed ticking both, so don't guess between them.
        rest = [v for v in vals if v not in ("Direct", "Fund")]
        return (["Direct"] + rest, "default, offer form listed both Direct and Fund",
                "Offer form listed both Direct and Fund; defaulted to Direct, please confirm.")
    return vals, "from offer form", None

def _offer_applies(offer, company_id, company_name, deal_type):
    """True when the deal being created is the one the web offer/bid describes."""
    if not offer:
        return False
    if offer.get("side") and str(deal_type or "").lower() not in ("", offer["side"]):
        return False
    if offer.get("company_id") and str(offer["company_id"]) == str(company_id):
        return True
    norm = lambda x: re.sub(r"[^a-z0-9]", "", (x or "").lower())
    return bool(offer.get("company")) and norm(offer["company"]) == norm(company_name)

def build_deal_name(company, min_size, max_size, deal_type):
    """Deterministic deal name: '{Company}: {Min} - {Max} {Buy|Sell}'.
    Min equal to max → '{Company}: {Max} {Side}'; min missing/zero → '{Company}: up to {Max} {Side}';
    max missing → '{Company}: {Min}+ {Side}'; no size at all → '{Company}: {Side}'."""
    side = "Buy" if str(deal_type or "").strip().lower() == "buy" else "Sell"
    lo, hi = _to_amount(min_size), _to_amount(max_size)
    lo = lo if lo and lo > 0 else None
    hi = hi if hi and hi > 0 else None
    if hi is not None and lo is not None and format_deal_amount(lo) != format_deal_amount(hi):
        size = f"{format_deal_amount(lo)} - {format_deal_amount(hi)}"
    elif hi is not None and lo is not None:
        size = format_deal_amount(hi)
    elif hi is not None:
        size = f"up to {format_deal_amount(hi)}"
    elif lo is not None:
        size = f"{format_deal_amount(lo)}+"
    else:
        size = None
    return f"{company}: {size} {side}" if size else f"{company}: {side}"

# ── Execute a tool call ───────────────────────────────────────────────────────
def execute_tool(tool_name, tool_input):
    logger.info(f"Tool: {tool_name}, Input: {json.dumps(tool_input)}")
    try:
        result = _execute_tool_inner(tool_name, tool_input)
        return result if result is not None else {"error": "Tool returned no result"}
    except Exception as e:
        logger.error(f"Tool error in {tool_name}: {e}")
        return {"error": str(e)}

def _format_deal(d):
    """Translate raw deal snapshot into labeled, human-readable fields."""
    cf = d.get("custom_fields", {}) or {}

    DEAL_TYPE_MAP = {5077819: "Buy Order", 5011675: "Sell Order"}
    raw_type = cf.get("custom_label_1958")
    deal_type = DEAL_TYPE_MAP.get(raw_type[0] if isinstance(raw_type, list) else raw_type)

    STRUCTURE_MAP = {6250090: "Direct", 5077906: "Fund/SPV", 5077903: "Forward Contract"}
    raw_structure = cf.get("custom_label_3064360")
    if isinstance(raw_structure, list):
        structure = " + ".join(STRUCTURE_MAP[s] for s in raw_structure if s in STRUCTURE_MAP) or None
    else:
        structure = STRUCTURE_MAP.get(raw_structure)

    CLASS_MAP = {5077831: "Common", 5077834: "Preferred", 5077912: "Mixed", 5077915: "Any"}
    deal_class = CLASS_MAP.get(cf.get("custom_label_3064330"))

    NEXUS_MAP = {6460632: "Direct", 6460633: "RMS Broker", 6460635: "Co-Broker", 6460634: "Foreign Finder"}
    nexus = NEXUS_MAP.get(cf.get("custom_label_3751449"))

    LAYERS_MAP = {7000228: "SPV on cap table", 7000229: "2-Layer SPV", 7000230: "3-Layer SPV"}
    layers = LAYERS_MAP.get(cf.get("custom_label_3938743"))

    SERIES_MAP = {5077843: "A", 5077846: "B", 5077849: "C", 5077852: "D", 5077855: "E",
                  5077858: "F", 5077861: "G", 5077864: "H", 5077867: "I", 5077918: "Mixed",
                  6539216: "N", 5077837: "N/A", 5077840: "Seed"}
    series = SERIES_MAP.get(cf.get("custom_label_3064333"))

    def pct(val):
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    return {
        "id": d.get("id"),
        "name": d.get("name"),
        "deal_stage": (d.get("deal_stage") or {}).get("name"),
        "deal_status": (d.get("deal_status") or {}).get("name"),
        "company": (d.get("company") or {}).get("name"),
        "company_id": d.get("company_id"),
        "primary_contact_id": d.get("primary_contact_id"),
        "updated_at": d.get("updated_at"),
        "created_at": d.get("created_at"),
        "deal_type": deal_type,
        "structure": structure,
        "class": deal_class,
        "series": series,
        "nexus": nexus,
        "layers": layers,
        "gross_price_per_share": cf.get("custom_label_3064339"),
        "net_price_per_share": cf.get("custom_label_3064369"),
        "min_deal_size": cf.get("custom_label_3065488"),
        "max_deal_size": cf.get("custom_label_3064645"),
        "num_shares": cf.get("custom_label_3070843"),
        "management_fee_pct": pct(cf.get("custom_label_3940558")),
        "carry_pct": pct(cf.get("custom_label_3940559")),
        "seller_fee_pct": pct(cf.get("custom_label_3940560")),
        "partner_fee_pct": pct(cf.get("custom_label_3940561")),
        "chad_commission_rate": cf.get("custom_label_3814251"),
        "seller_legal_name": cf.get("custom_label_3064355"),
        "buyer_legal_name": cf.get("custom_label_3064356"),
        "private_notes": cf.get("custom_label_3064357"),
        "refresh_days": cf.get("custom_label_3994687"),
        "pipeline_url": f"https://app.pipelinecrm.com/deals/{d.get('id')}",
    }

_COMPANY_NOISE_WORDS = {
    "inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation", "co", "company",
    "holdings", "holding", "group", "technologies", "technology", "tech", "labs", "lab",
    "laboratories", "systems", "system", "robotics", "dynamics", "industries", "networks",
    "network", "computing", "sciences", "science", "bio", "health", "space", "energy",
    "motors", "software", "solutions", "ventures", "partners", "capital", "ai", "plc",
    "gmbh", "sa", "nv", "bv", "ab", "oy", "pte", "pty", "and", "the",
}


def _company_tokens(name):
    """Lowercased token list with punctuation and generic corporate words removed."""
    raw = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    toks = [t for t in raw.split() if t]
    core = [t for t in toks if t not in _COMPANY_NOISE_WORDS]
    return core or toks


def _company_match_score(query, name):
    """Lower score = closer match. None = no match."""
    q = (query or "").lower().strip()
    n = (name or "").lower().strip()
    if not q or not n:
        return None
    if q == n:
        return 0
    qt, nt = _company_tokens(q), _company_tokens(n)
    if qt and nt:
        if qt == nt:
            return 1
        short, long_ = (qt, nt) if len(qt) <= len(nt) else (nt, qt)
        if long_[:len(short)] == short:
            return 2
    if q in n:
        return 3
    if n in q and len("".join(_company_tokens(n))) >= 4:
        return 4
    return None


def _execute_tool_inner(tool_name, tool_input):

    if tool_name == "search_companies":
        query = tool_input['name'].lower().strip()
        companies = get_snapshot("companies.json")
        scored = []
        for c in companies:
            s = _company_match_score(query, c.get("name"))
            if s is not None:
                scored.append((s, len(c.get("name") or ""), c))
        scored.sort(key=lambda x: (x[0], x[1]))
        matches = [c for _, _, c in scored[:10]]
        return [{"id": c["id"], "name": c["name"], "custom_fields": c.get("custom_fields", {})} for c in matches] or {"results": [], "message": "No matches found"}

    elif tool_name == "get_company":
        cid = int(tool_input['company_id'])
        companies = get_snapshot("companies.json")
        for c in companies:
            if c.get("id") == cid:
                return c
        # Fallback to live API
        result = call_pipeline_api("GET", f"/companies/{cid}.json")
        return result["data"] if result["status"] == 200 else {"error": f"Company {cid} not found"}

    elif tool_name == "search_people":
        query = tool_input['query'].lower().strip()
        people = get_snapshot("people.json")
        matches = []
        for p in people:
            emails = {(p.get(f) or "").lower().strip() for f in ("email", "email2", "home_email")}
            emails.discard("")
            name = f"{p.get('first_name','')} {p.get('last_name','')}".lower().strip()
            company = (p.get("company_name") or "").lower()
            if '@' in query:
                if query in emails:
                    matches.append(p)
            else:
                if query in name or query in company:
                    matches.append(p)
            if len(matches) >= 10:
                break
        return [{"id": p["id"], "first_name": p.get("first_name"), "last_name": p.get("last_name"),
                 "email": p.get("email"), "company_name": p.get("company_name"),
                 "custom_fields": p.get("custom_fields", {})} for p in matches] or {"results": [], "message": "No matches found"}

    elif tool_name == "get_person":
        pid = int(tool_input['person_id'])
        result = call_pipeline_api("GET", f"/people/{pid}.json")
        if result["status"] == 200:
            return result["data"]
        people = get_snapshot("people.json")
        for p in people:
            if p.get("id") == pid:
                return p
        return {"error": f"Person {pid} not found"}

    elif tool_name == "search_deals":
        if not any(k in tool_input for k in ["company_id", "person_id", "name"]):
            return {"error": "search_deals requires at least one of: company_id, person_id, or name."}
        deals = get_snapshot("deals.json")
        filtered = []
        for d in deals:
            if "company_id" in tool_input and d.get("company_id") != tool_input["company_id"]:
                continue
            if "person_id" in tool_input and d.get("primary_contact_id") != tool_input["person_id"]:
                continue
            if "name" in tool_input and tool_input["name"].lower() not in (d.get("name") or "").lower():
                continue
            filtered.append(d)
        result_deals = [_format_deal(d) for d in filtered[:tool_input.get('per_page', 20)]]
        return {"total": len(filtered), "deals": result_deals}


    elif tool_name == "get_deal":
        did = int(tool_input['deal_id'])
        deals = get_snapshot("deals.json")
        for d in deals:
            if d.get("id") == did:
                return _format_deal(d)
        # Fallback to live API
        result = call_pipeline_api("GET", f"/deals/{did}.json")
        return _format_deal(result["data"]) if result["status"] == 200 else {"error": f"Deal {did} not found"}

    elif tool_name == "create_person":
        # Hard duplicate check — only block if the returned record actually has this exact email
        email = tool_input.get("email")
        if email:
            new_email = email.lower().strip()
            dup = None
            # Live check — catches very recent records matched on the PRIMARY email field
            check = call_pipeline_api("GET", f"/people.json?conditions[email]={urllib.parse.quote(email)}&per_page=5")
            if check["status"] == 200 and check["data"].get("entries"):
                for existing in check["data"]["entries"]:
                    if (existing.get("email") or "").lower().strip() == new_email:
                        dup = existing
                        break
            # Snapshot check — catches the address living in ANY email slot (email / email2 / home_email)
            if not dup:
                for existing in get_snapshot("people.json"):
                    slots = {(existing.get(f) or "").lower().strip() for f in ("email", "email2", "home_email")}
                    if new_email in slots:
                        dup = existing
                        break
            if dup:
                return {
                    "duplicate": True,
                    "message": f"Person with email {email} already exists. Do NOT create a duplicate. Update the existing record instead.",
                    "existing_person_id": dup["id"],
                    "existing_name": f"{dup.get('first_name','')} {dup.get('last_name','')}".strip(),
                    "pipeline_url": f"https://app.pipelinecrm.com/people/{dup['id']}"
                }
        tool_input = dict(tool_input)
        phone_country = tool_input.pop("phone_country", None) or tool_input.get("work_country") or tool_input.get("home_country")
        phone_flags = guard_phone_fields(tool_input, phone_country)
        # Build person payload from provided fields
        person_data = {}
        standard_fields = ["first_name", "last_name", "email", "home_email", "phone", "mobile",
                           "company_name", "position", "linked_in_url", "website",
                           "summary", "work_address_1", "work_address_2", "work_city",
                           "work_country", "work_state", "work_postal_code",
                           "work_street", "work_phone",
                           "home_address_1", "home_address_2", "home_city",
                           "home_country", "home_state", "home_postal_code",
                           "home_email", "home_phone", "source_id"]
        custom_fields = {}
        for field, value in tool_input.items():
            if not value:
                continue
            if field in standard_fields:
                person_data[field] = value
            elif field.startswith("custom_label_"):
                custom_fields[field] = value
        # Detect support-staff titles (EA, assistant, secretary, etc.) → mark as Contact instead of Lead
        SUPPORT_TITLE_PATTERNS = [
            r"\bexecutive\s+assistant\b", r"\bexec\.?\s+assistant\b", r"\bexecutive\s+asst\b",
            r"\bpersonal\s+assistant\b", r"\badmin(?:istrative)?\s+assistant\b",
            r"\bassistant\s+to\b", r"\bassistant\b", r"\basst\b",
            r"\bsecretary\b", r"\breceptionist\b", r"\bscheduler\b",
            r"\boffice\s+(?:manager|admin(?:istrator)?)\b", r"\badministrator\b",
            r"\bsupport\s+staff\b", r"\bea\b", r"\bpa\b",
        ]
        position_raw = (person_data.get("position") or "").strip()
        position_lc = position_raw.lower()
        is_support = bool(position_lc) and any(re.search(p, position_lc) for p in SUPPORT_TITLE_PATTERNS)

        if is_support:
            person_data["type"] = "Contact"
        else:
            person_data["type"] = "Lead"
            person_data["predefined_contacts_tag_ids"] = [3280123]
        result = call_pipeline_api("POST", "/people.json", {"person": person_data})
        if result["status"] == 200:
            new_id = result["data"].get("id")
            pipeline_url = f"https://app.pipelinecrm.com/people/{new_id}"
            # Step 2: update custom fields separately via PUT (more reliable than POST)
            if custom_fields:
                MULTI_SELECT_FIELDS = {"custom_label_3322093", "custom_label_3759156", "custom_label_3740611", "custom_label_3052210", "custom_label_3998063"}
                formatted_cf = {}
                for k, v in custom_fields.items():
                    if k in MULTI_SELECT_FIELDS and isinstance(v, list):
                        formatted_cf[k] = [int(x) for x in v if str(x).strip().isdigit()]
                    else:
                        formatted_cf[k] = v
                call_pipeline_api("PUT", f"/people/{new_id}.json", {"person": {"custom_fields": formatted_cf}})
            if is_support:
                note = (f"NOTE: Title '{position_raw}' looks like support staff, so this person was created "
                        f"as a Contact (not a Lead) and the Whitelist tag was not applied. "
                        f"Include this note in your reply to Chad so he can override if needed.")
                return {"success": True, "person_id": new_id, "pipeline_url": pipeline_url,
                        "person_type": "Contact", "support_staff_detected": True,
                        "message": f"Created as Contact. {note} Reply must include the pipeline_url so Chad can verify."}
            out = {"success": True, "person_id": new_id, "pipeline_url": pipeline_url, "person_type": "Lead", "message": f"Created. Reply must include the pipeline_url so Chad can verify."}
            if phone_flags:
                out["phone_flags"] = phone_flags
                out["message"] += " Some phone values were NOT written — report phone_flags to Chad."
            return out
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "update_person":
        fields = dict(tool_input["fields"])
        phone_country = fields.pop("phone_country", None) or fields.get("work_country") or fields.get("home_country")
        phone_flags = guard_phone_fields(fields, phone_country)
        # Entity IQF/CEF: the Principal Place of Business belongs on the linked company
        # (update_company), never on the contact person's work/home address.
        blocked_address, entity_trigger = [], None
        if any(k in PERSON_ADDRESS_FIELDS for k in fields):
            entity_trigger = entity_form_trigger()
        if entity_trigger:
            blocked_address = sorted(k for k in fields if k in PERSON_ADDRESS_FIELDS)
            for k in blocked_address:
                fields.pop(k)
            logger.warning(f"entity-form guard: person {tool_input['person_id']} — dropped {blocked_address}; "
                           f"triggered by {entity_trigger}")
        # Split standard fields from custom fields
        standard = {}
        custom = {}
        for k, v in fields.items():
            if k.startswith("custom_label_"):
                custom[k] = v
            else:
                standard[k] = v
        # Multi-select fields merge with what's already on the record so we never
        # clobber other selections. To DELETE a value, send custom_label_X__remove
        # with the entry IDs to drop; the final set is (existing + added) minus removed.
        MULTI_SELECT_FIELDS = {"custom_label_3322093", "custom_label_3759156", "custom_label_3740611", "custom_label_3052210", "custom_label_3998063"}
        def _id_list(v):
            if isinstance(v, list):
                return [int(x) for x in v if str(x).strip().isdigit()]
            if isinstance(v, int):
                return [v]
            if isinstance(v, str):
                return [int(x) for x in v.replace("|", ",").split(",") if x.strip().isdigit()]
            return []
        # Pull out removal directives (custom_label_X__remove) before building the payload
        removals = {}
        for k in [k for k in custom if k.endswith("__remove")]:
            removals[k[:-len("__remove")]] = custom.pop(k)
        affected = {k for k in custom if k in MULTI_SELECT_FIELDS} | {b for b in removals if b in MULTI_SELECT_FIELDS}
        if affected:
            existing = call_pipeline_api("GET", f"/people/{tool_input['person_id']}.json")
            if existing["status"] == 200:
                existing_cf = existing["data"].get("custom_fields", {})
                for k in affected:
                    remove_ids = set(_id_list(removals.get(k)))
                    merged = _id_list(existing_cf.get(k)) + _id_list(custom.get(k))
                    custom[k] = [i for i in dict.fromkeys(merged) if i not in remove_ids]
        blocked_downgrades = guard_form_status_downgrades(tool_input["person_id"], custom)
        notes = {}
        if blocked_downgrades:
            notes["blocked_downgrades"] = blocked_downgrades
        if phone_flags:
            notes["phone_flags"] = phone_flags
        if blocked_address:
            notes["blocked_address_fields"] = blocked_address
            notes["entity_guard_fired"] = (f"Entity-form guard dropped person address fields {blocked_address} "
                                           f"(detected from {entity_trigger}). Mention this in your reply so Chad "
                                           f"can catch a false positive.")
            notes["entity_form_address"] = ("Entity form: person address fields were NOT written. Write the Principal Place "
                                            "of Business to the linked company with update_company (address_1, city, state, "
                                            "postal_code, country) — see RULE 20.")
        if not standard and not custom:
            return {"success": True, **notes,
                    "message": "No fields written — every requested change was blocked. Report the blocked items to the user."}
        payload = {"person": standard}
        if custom:
            payload["person"]["custom_fields"] = custom
        result = call_pipeline_api("PUT", f"/people/{tool_input['person_id']}.json", payload)
        if result["status"] == 200:
            out = {"success": True, "message": "Record updated. Do not verify or retry — report success to the user.", **notes}
            if blocked_downgrades or phone_flags or blocked_address:
                out["message"] += (" Some fields were NOT changed — report blocked_downgrades / phone_flags / "
                                   "entity_guard_fired to the user.")
            return out
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "update_company":
        COMPANY_FIELD_MAP = {
            "last_round_valuation": "custom_label_3790429",
            "hiive_bid":            "custom_label_3997298",
            "hiive_ask":            "custom_label_3997297",
        }
        fields = dict(tool_input["fields"])
        # Common aliases → Pipeline company address keys
        for alias, key in (("address", "address_1"), ("street", "address_1"), ("zip", "postal_code"),
                           ("postal", "postal_code"), ("phone", "phone1")):
            if alias in fields and key not in fields:
                fields[key] = fields.pop(alias)
        phone_country = fields.pop("phone_country", None) or fields.get("country")
        phone_flags = guard_phone_fields(fields, phone_country,
                                         keys=[k for k in fields if "phone" in k and not k.startswith("custom_label_")])
        notes = {}
        if phone_flags:
            notes["phone_flags"] = phone_flags
        entity_trigger = entity_form_trigger() if "name" in fields else None
        if entity_trigger:
            new_name = fields.pop("name")
            logger.warning(f"entity-form guard: company {tool_input['company_id']} — rename to {new_name!r} blocked; "
                           f"triggered by {entity_trigger}")
            notes["rename_blocked"] = (f"Entity form: company NOT renamed to {new_name!r}. "
                                       "Report 'Company name mismatch' to Chad instead (RULE 20).")
            notes["entity_guard_fired"] = (f"Entity-form guard blocked renaming company {tool_input['company_id']} "
                                           f"(detected from {entity_trigger}). Mention this in your reply so Chad "
                                           f"can catch a false positive.")
        address_keys = [k for k in fields if k in COMPANY_ADDRESS_FIELDS
                         or ("phone" in k and not k.startswith("custom_label_"))]
        if address_keys:
            live = call_pipeline_api("GET", f"/companies/{tool_input['company_id']}.json")
            if live["status"] == 200 and isinstance(live["data"], dict):
                notes["previous_address"] = {k: live["data"].get(k) for k in COMPANY_ADDRESS_FIELDS}
        standard = {}
        custom = {}
        for k, v in fields.items():
            if k in COMPANY_FIELD_MAP:
                custom[COMPANY_FIELD_MAP[k]] = v
            elif k.startswith("custom_label_"):
                custom[k] = v
            else:
                standard[k] = v
        if tool_input.get("industry"):
            industry_ids = [INDUSTRY_MAP[i] for i in tool_input["industry"] if i in INDUSTRY_MAP]
            if industry_ids:
                custom["custom_label_3065122"] = industry_ids
        # For multi-select fields, fetch existing and merge — send as JSON arrays
        COMPANY_MULTI_SELECT = {"custom_label_3749627", "custom_label_3749628", "custom_label_3746654", "custom_label_3065122"}
        logger.info(f"update_company custom keys: {list(custom.keys())}")
        today = date.today().strftime("%Y-%m-%d")
        if HIIVE_BID_FIELD in custom or "custom_label_3997298" in custom:
            custom[HIIVE_BID_DATE_FIELD] = today
            custom["custom_label_3997300"] = today
        if HIIVE_ASK_FIELD in custom or "custom_label_3997297" in custom:
            custom[HIIVE_ASK_DATE_FIELD] = today
            custom["custom_label_3997299"] = today
        if any(k in COMPANY_MULTI_SELECT for k in custom):
            existing = call_pipeline_api("GET", f"/companies/{tool_input['company_id']}.json")
            if existing["status"] == 200:
                existing_cf = existing["data"].get("custom_fields", {})
                for k, v in list(custom.items()):
                    if k not in COMPANY_MULTI_SELECT:
                        continue
                    if isinstance(v, list):
                        new_ids = [int(x) for x in v if str(x).strip().isdigit()]
                    elif isinstance(v, int):
                        new_ids = [v]
                    elif isinstance(v, str):
                        new_ids = [int(x) for x in v.replace("|", ",").split(",") if x.strip().isdigit()]
                    else:
                        continue
                    existing_vals = existing_cf.get(k, [])
                    if isinstance(existing_vals, list):
                        existing_ids = [int(x) for x in existing_vals if str(x).strip().isdigit()]
                    elif existing_vals:
                        existing_ids = [int(x) for x in str(existing_vals).replace("|", ",").split(",") if x.strip().isdigit()]
                    else:
                        existing_ids = []
                    custom[k] = list(dict.fromkeys(existing_ids + new_ids))
        payload = {"company": standard}
        if custom:
            payload["company"]["custom_fields"] = custom
        if not standard and not custom:
            return {"success": True, **notes, "message": "No fields written. Report the notes to the user."}
        result, unknown = put_company_checked(tool_input["company_id"], payload, address_keys)
        if unknown:
            notes["unknown_address_fields"] = unknown
        if result["status"] == 200:
            return {"success": True, **notes, "message": "Record updated. Do not verify or retry — report success (and any phone_flags / rename_blocked / entity_guard_fired / unknown_address_fields) to the user."}
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "create_company":
        company_data = {"name": tool_input["name"]}
        if tool_input.get("summary"):
            company_data["description"] = tool_input["summary"]
        if tool_input.get("website"):
            company_data["web"] = tool_input["website"]
        if tool_input.get("address"):
            company_data["address_1"] = tool_input["address"]
        custom_fields = {}
        if tool_input.get("legal_name"):
            custom_fields["custom_label_3769275"] = tool_input["legal_name"]
        if tool_input.get("org_type"):
            custom_fields["custom_label_625142"] = tool_input["org_type"]
        if tool_input.get("last_round_pps") is not None:
            custom_fields["custom_label_3064363"] = tool_input["last_round_pps"]
        if tool_input.get("last_round_valuation") is not None:
            custom_fields["custom_label_3790429"] = tool_input["last_round_valuation"]
        if tool_input.get("last_round_date"):
            custom_fields["custom_label_3826032"] = tool_input["last_round_date"]
        if tool_input.get("last_round_series"):
            custom_fields["custom_label_3914626"] = tool_input["last_round_series"]
        if tool_input.get("industry"):
            industry_ids = [INDUSTRY_MAP[i] for i in tool_input["industry"] if i in INDUSTRY_MAP]
            if industry_ids:
                custom_fields["custom_label_3065122"] = industry_ids
        if custom_fields:
            company_data["custom_fields"] = custom_fields
        result = call_pipeline_api("POST", "/companies.json", {"company": company_data})
        if result["status"] == 200:
            new_id = result["data"].get("id")
            return {
                "success": True,
                "company_id": new_id,
                "pipeline_url": f"https://app.pipelinecrm.com/companies/{new_id}",
                "message": "Company created successfully."
            }
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "add_note":
        payload = {"note": {"content": tool_input["note"],
                            "note_category_id": tool_input.get("note_category_id", 69759),
                            "person_id": tool_input.get("person_id"),
                            "company_id": tool_input.get("company_id"),
                            "deal_id": tool_input.get("deal_id")}}
        result = call_pipeline_api("POST", "/notes.json", payload)
        return {"success": result["status"] == 200, "status": result["status"]}

    elif tool_name == "get_security_ids":
        def format_ids(k, v):
            return {
                "security": k,
                "holding_entry_id": v.get("h"),
                "buy_interest_entry_id": v.get("b"),
                "sell_interest_entry_id": v.get("s"),
                "usage": "Use buy_interest_entry_id when adding to custom_label_3322093 (people buying), sell_interest_entry_id for custom_label_3759156 (people selling), holding_entry_id for custom_label_3740611 (people holding)"
            }
        def snapshot_lookup(name):
            if name in SECURITY_IDS:
                return format_ids(name, SECURITY_IDS[name])
            name_lower = name.lower()
            for k, v in SECURITY_IDS.items():
                if k.lower() == name_lower:
                    return format_ids(k, v)
            matches = [(k, v) for k, v in SECURITY_IDS.items() if name_lower in k.lower()]
            if matches:
                return {"matches": [format_ids(k, v) for k, v in matches[:5]]}
            return None
        def legal_name_retry(name):
            companies = get_snapshot("companies.json")
            name_lower = name.lower()
            for c in companies:
                cname = (c.get("name") or "")
                legal = (c.get("custom_fields", {}) or {}).get("custom_label_3769275") or ""
                if not legal or legal.lower() == cname.lower():
                    continue
                if name_lower == cname.lower():
                    alt = snapshot_lookup(legal)
                    if alt is not None:
                        return alt
                if name_lower == legal.lower():
                    alt = snapshot_lookup(cname)
                    if alt is not None:
                        return alt
            return None
        def lookup_one(name):
            result = snapshot_lookup(name)
            if result is not None:
                return result
            if _refresh_security_ids_from_api():
                result = snapshot_lookup(name)
                if result is not None:
                    return result
            alt = legal_name_retry(name)
            if alt is not None:
                return alt
            return {"error": f"Security '{name}' not found in lookup table"}
        names = tool_input.get("security_names")
        if names:
            return {"results": [lookup_one(n) for n in names]}
        return lookup_one(tool_input.get("security_name", ""))

    elif tool_name == "add_security":
        name = (tool_input.get("security_name") or "").strip()
        if not name:
            return {"error": "security_name is required"}
        if not SECURITY_IDS:
            _refresh_security_ids_from_api()
        def _norm(s):
            return s.lower().rstrip("$#").strip()
        target = _norm(name)
        force = bool(tool_input.get("force"))
        STOP_TOKENS = {"ai", "inc", "labs", "lab", "the", "co", "corp", "llc", "ltd", "tech", "technologies", "group", "holdings", "company", "capital"}
        def _tokens(s):
            return {t for t in re.split(r"[^a-z0-9]+", _norm(s)) if t and t not in STOP_TOKENS}
        def _edit_dist(a, b, cap=3):
            if abs(len(a) - len(b)) > cap - 1:
                return cap
            prev = list(range(len(b) + 1))
            for i, ca in enumerate(a, 1):
                cur = [i]
                for j, cb in enumerate(b, 1):
                    cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
                prev = cur
            return prev[-1]
        t_tokens = _tokens(name)
        near = []
        for k in SECURITY_IDS:
            kn = _norm(k)
            if kn == target or (t_tokens and t_tokens & _tokens(k)) or (min(len(kn), len(target)) >= 5 and _edit_dist(kn, target) <= 2):
                near.append(k)
        if near and not force:
            return {"error": f"Refusing to create '{name}': near-match already exists", "existing": near[:5], "hint": "Use get_security_ids with the existing name. If Chad explicitly confirms this is a different security (says 'override duplicate check'), retry with force: true and disclose the bypassed match in your reply."}
        field_roles = {3322093: "b", 3759156: "s", 3740611: "h"}
        created = {"h": None, "b": None, "s": None}
        errors = []
        for label_id, role in field_roles.items():
            payload = {"custom_field_label_dropdown_entry": {"custom_field_label_id": label_id, "name": name}}
            result = call_pipeline_api("POST", "/admin/custom_field_label_dropdown_entries.json", payload)
            if result.get("status") == 200 and isinstance(result.get("data"), dict):
                entry = result["data"].get("custom_field_label_dropdown_entry") or result["data"]
                eid = entry.get("id")
                if eid:
                    created[role] = int(eid)
                    continue
            errors.append({"field_id": label_id, "status": result.get("status"), "data": str(result.get("data"))[:200]})
        if any(created.values()):
            SECURITY_IDS[name] = created
        if errors:
            if all(e.get("status") in (401, 403) for e in errors):
                return {"error": "API credentials lack admin rights for dropdown entry creation (401/403). Tell Chad.", "details": errors}
            return {"partial": created, "errors": errors, "warning": "Security is half-created across fields — report this to Chad, do not proceed silently."}
        return {"created": name, "holding_entry_id": created["h"], "buy_interest_entry_id": created["b"], "sell_interest_entry_id": created["s"], "note": "New entries appear at the bottom of the Pipeline dropdown UI until re-sorted."}

    elif tool_name == "search_people_by_interest":
        field_map = {"holding": "custom_label_3740611", "buy": "custom_label_3322093", "sell": "custom_label_3759156"}
        field_key = field_map.get(tool_input["field_type"])
        if not field_key:
            return {"error": "field_type must be holding, buy, or sell"}
        entry_id = int(tool_input["entry_id"])
        per_page = tool_input.get("per_page", 20)
        people = get_snapshot("people.json")
        matches = []
        for p in people:
            cf = p.get("custom_fields", {})
            val = cf.get(field_key, [])
            ids = val if isinstance(val, list) else [val] if val else []
            if entry_id in [int(x) for x in ids if x]:
                matches.append({"id": p["id"],
                                 "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                                 "email": p.get("email"), "company": p.get("company_name")})
            if len(matches) >= per_page:
                break
        return matches or {"results": [], "message": "No matches found"}

    elif tool_name == "search_companies_by_interest":
        field_map = {"holding": "custom_label_3746654", "buy": "custom_label_3749627", "sell": "custom_label_3749628"}
        field_key = field_map.get(tool_input["field_type"])
        if not field_key:
            return {"error": "field_type must be holding, buy, or sell"}
        entry_id = int(tool_input["entry_id"])
        per_page = tool_input.get("per_page", 20)
        companies = get_snapshot("companies.json")
        matches = []
        for c in companies:
            cf = c.get("custom_fields", {})
            val = cf.get(field_key, [])
            ids = val if isinstance(val, list) else [val] if val else []
            if entry_id in [int(x) for x in ids if x]:
                matches.append({"id": c["id"], "name": c["name"]})
            if len(matches) >= per_page:
                break
        return matches or {"results": [], "message": "No matches found"}

    elif tool_name == "find_investors":
        interest_type = tool_input.get("interest_type", "buy").lower()
        min_ticket = tool_input.get("min_ticket")
        limit = tool_input.get("limit", 25)
        extra_red_flags = tool_input.get("red_flag_terms", [])

        # Build list of security names to search
        raw_names = tool_input.get("security_names") or []
        if tool_input.get("security_name"):
            raw_names = [tool_input["security_name"]] + [n for n in raw_names if n != tool_input["security_name"]]
        if not raw_names:
            return {"error": "Provide security_name or security_names"}

        # Ticket size entry IDs → max dollar value for filtering
        TICKET_MAX = {
            6870210: 100000,
            6631962: 499000,
            5014552: 1000000,
            5014555: 5000000,
            5014558: 10000000,
            5014561: 25000000,
            5014564: 50000000,
            5014567: 100000000,
        }

        RED_FLAG_TERMS = [
            "unreliable", "unresponsive", "do not contact", "difficult",
            "ghosted", "scam", "not sophisticated", "no spv", "no spvs",
            "flaky", "doesn't seem professional"
        ] + [t.lower() for t in extra_red_flags]

        PRIORITY_HIGH = 6919452
        PRIORITY_MEDIUM = 6919453
        PRIORITY_LOW = 6919454
        PRIORITY_NONE = 6926715

        # Resolve all security names → entry IDs
        if interest_type == "buy":
            field_key = "custom_label_3322093"
            id_key = "b"
        elif interest_type == "sell":
            field_key = "custom_label_3759156"
            id_key = "s"
        else:
            return {"error": "interest_type must be 'buy' or 'sell'"}

        entry_ids = set()
        resolved_names = []
        not_found = []
        for raw_name in raw_names:
            name = raw_name.strip()
            if name not in SECURITY_IDS:
                name_lower = name.lower()
                matched = next((k for k in SECURITY_IDS if k.lower() == name_lower), None)
                if not matched:
                    not_found.append(name)
                    continue
                name = matched
            eid = SECURITY_IDS[name].get(id_key)
            if eid:
                entry_ids.add(eid)
                resolved_names.append(name)
            else:
                not_found.append(name)

        if not entry_ids:
            return {"error": f"No securities found: {not_found}"}

        # Single pass over people snapshot — match ANY of the entry IDs
        people = get_snapshot("people.json")
        candidates = []

        for p in people:
            cf = p.get("custom_fields", {})
            val = cf.get(field_key, [])
            ids_list = val if isinstance(val, list) else [val] if val else []
            person_entry_ids = {int(x) for x in ids_list if x}
            if not (person_entry_ids & entry_ids):
                continue

            # Which securities matched (for display)
            matched_securities = [
                name for name, sec_ids in SECURITY_IDS.items()
                if sec_ids.get(id_key) in person_entry_ids and sec_ids.get(id_key) in entry_ids
            ]

            # Hard exclude intermediaries
            role = cf.get("custom_label_3075382")
            if role == 6438705:
                continue

            # Priority filtering
            priority_raw = cf.get("custom_label_3912746")
            if isinstance(priority_raw, list):
                priority_raw = priority_raw[0] if priority_raw else None
            try:
                priority_id = int(priority_raw) if priority_raw is not None else None
            except (ValueError, TypeError):
                priority_id = None

            if priority_id in (PRIORITY_LOW, PRIORITY_NONE):
                continue

            if priority_id == PRIORITY_HIGH:
                priority_label = "High"
            elif priority_id == PRIORITY_MEDIUM:
                priority_label = "Medium"
            else:
                priority_label = None  # unset

            # Ticket size filtering
            if min_ticket:
                ticket_ids = cf.get("custom_label_3052210", [])
                if not isinstance(ticket_ids, list):
                    ticket_ids = [ticket_ids] if ticket_ids else []
                if ticket_ids:
                    person_max_ticket = max(
                        (TICKET_MAX.get(int(t), 0) for t in ticket_ids if t),
                        default=0
                    )
                    if person_max_ticket > 0 and person_max_ticket < min_ticket:
                        continue

            # Summary red flags
            summary = (p.get("summary") or "").lower()
            if any(flag in summary for flag in RED_FLAG_TERMS):
                continue

            # Whitelist check
            tags = p.get("predefined_contacts_tag_ids") or []
            whitelisted = 3280123 in [int(t) for t in tags if str(t).strip().isdigit()]

            # Ticket size display
            ticket_ids = cf.get("custom_label_3052210", [])
            if not isinstance(ticket_ids, list):
                ticket_ids = [ticket_ids] if ticket_ids else []
            ticket_labels = {
                6870210: "<$100K", 6631962: "$100K–$499K", 5014552: "$500K–$1M",
                5014555: "$1M–$5M", 5014558: "$5M–$10M", 5014561: "$10M–$25M",
                5014564: "$25M–$50M", 5014567: "$50M–$100M"
            }
            ticket_display = ", ".join(
                ticket_labels.get(int(t), str(t)) for t in ticket_ids if t
            ) or "unset"

            candidates.append({
                "id": p["id"],
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "email": p.get("email", ""),
                "company": p.get("company_name", ""),
                "city": p.get("work_city", ""),
                "country": p.get("work_country", ""),
                "priority": priority_label or "unset",
                "whitelisted": whitelisted,
                "ticket_size": ticket_display,
                "matched_securities": matched_securities,
                "summary_snippet": (p.get("summary") or "")[:200],
            })

        # Rank: High+whitelisted → Medium+whitelisted → unset+whitelisted → cold
        def rank_key(c):
            p = c["priority"]
            p_order = 0 if p == "High" else 1 if p == "Medium" else 2
            w_order = 0 if c["whitelisted"] else 1
            return (p_order, w_order, c["name"])

        candidates.sort(key=rank_key)

        # Deduplicate by id
        seen = set()
        deduped = []
        for c in candidates:
            if c["id"] not in seen:
                seen.add(c["id"])
                deduped.append(c)

        total_found = len(deduped)
        results = deduped[:limit]

        logger.info(f"find_investors: {resolved_names} {interest_type} — {total_found} candidates after filtering, returning {len(results)}")

        return {
            "searched_securities": resolved_names,
            "not_found": not_found if not_found else None,
            "total_after_filtering": total_found,
            "returning": len(results),
            "investors": results
        }

    elif tool_name == "compare_pdf_contacts":
        import re as _re
        pdf_text = tool_input.get("pdf_text", "")
        security_name = tool_input.get("security_name", "").strip()
        interest_type = tool_input.get("interest_type", "holding").lower()

        pdf_emails = set(e.lower().strip() for e in _re.findall(r'[\w.+%-]+@[\w.-]+\.[a-zA-Z]{2,}', pdf_text))

        pdf_names = set()
        for line in pdf_text.splitlines():
            line = line.strip()
            words = line.split()
            if 2 <= len(words) <= 4 and all(w[0].isupper() for w in words if w.isalpha()):
                pdf_names.add(line.lower())

        people = get_snapshot("people.json")
        email_index = {}
        name_index = {}

        for p in people:
            email = (p.get("email") or "").lower().strip()
            if email:
                email_index[email] = p
            full_name = f"{p.get('first_name','')} {p.get('last_name','')}".strip().lower()
            if full_name:
                name_index[full_name] = p

        interest_ids = set()
        interest_field = None
        if security_name:
            if security_name not in SECURITY_IDS:
                name_lower = security_name.lower()
                security_name = next((k for k in SECURITY_IDS if k.lower() == name_lower), security_name)
            sec = SECURITY_IDS.get(security_name, {})
            id_key = {"holding": "h", "buy": "b", "sell": "s"}.get(interest_type, "h")
            eid = sec.get(id_key)
            if eid:
                interest_ids.add(eid)
            interest_field = {"holding": "custom_label_3740611", "buy": "custom_label_3322093", "sell": "custom_label_3759156"}.get(interest_type)

        def has_interest(p):
            if not interest_ids or not interest_field:
                return None
            cf = p.get("custom_fields", {})
            val = cf.get(interest_field, [])
            ids = val if isinstance(val, list) else [val] if val else []
            return bool(interest_ids & {int(x) for x in ids if x})

        matched = []
        not_found = []
        seen_ids = set()

        for email in pdf_emails:
            p = email_index.get(email)
            if p and p["id"] not in seen_ids:
                seen_ids.add(p["id"])
                matched.append({
                    "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                    "email": p.get("email"),
                    "company": p.get("company_name", ""),
                    "pipeline_url": f"https://app.pipelinecrm.com/people/{p['id']}",
                    "has_interest": has_interest(p),
                    "match_type": "email"
                })
            elif not p:
                not_found.append({"email": email, "name": None})

        for name in pdf_names:
            p = name_index.get(name)
            if p and p["id"] not in seen_ids:
                seen_ids.add(p["id"])
                matched.append({
                    "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                    "email": p.get("email"),
                    "company": p.get("company_name", ""),
                    "pipeline_url": f"https://app.pipelinecrm.com/people/{p['id']}",
                    "has_interest": has_interest(p),
                    "match_type": "name"
                })

        if security_name and interest_field:
            already_marked = [m for m in matched if m["has_interest"]]
            not_marked = [m for m in matched if not m["has_interest"]]
        else:
            already_marked = matched
            not_marked = []

        logger.info(f"compare_pdf_contacts: {len(pdf_emails)} emails + {len(pdf_names)} names from PDF → {len(matched)} CRM matches, {len(not_found)} not found")

        return {
            "pdf_emails_found": len(pdf_emails),
            "pdf_names_found": len(pdf_names),
            "in_crm": len(matched),
            "not_in_crm": len(not_found),
            "already_marked_interest": already_marked if security_name else None,
            "in_crm_not_marked": not_marked if security_name else None,
            "not_in_crm": not_found,
            "security_checked": security_name or None,
            "interest_type_checked": interest_type if security_name else None,
        }

    elif tool_name == "web_search":
        query = tool_input.get("query", "").strip()
        if not query:
            return {"error": "No query provided"}
        try:
            search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
            req = urllib.request.Request(search_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            })
            with urllib.request.urlopen(req, timeout=8) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            import re as _re
            # Extract result titles, snippets and URLs
            results = []
            # Find result blocks
            blocks = _re.findall(r'<a class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>.*?<a class="result__snippet"[^>]*>(.*?)</a>', raw, _re.DOTALL)
            for url, title, snippet in blocks[:8]:
                title = _re.sub(r'<[^>]+>', '', title).strip()
                snippet = _re.sub(r'<[^>]+>', '', snippet).strip()
                results.append({"title": title, "snippet": snippet, "url": url})
            if not results:
                # Fallback: just extract visible text
                text = _re.sub(r'<[^>]+>', ' ', raw)
                text = _re.sub(r'\s+', ' ', text).strip()
                return {"results": [], "raw_text": text[:2000]}
            return {"results": results}
        except Exception as e:
            return {"error": str(e)}

    elif tool_name == "fetch_webpage":
        url = tool_input.get("url", "").strip()
        if not url.startswith("http"):
            url = "https://" + url
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            # Strip HTML tags
            import re as _re
            text = _re.sub(r'<[^>]+>', ' ', raw)
            text = _re.sub(r'\s+', ' ', text).strip()
            return {"url": url, "content": text[:3000]}
        except Exception as e:
            return {"error": str(e), "url": url}

    elif tool_name == "create_deal":
        # Live duplicate-deal guard — the deals.json snapshot is hourly and can miss recent deals.
        try:
            company_id = int(tool_input["company_id"])
            primary_contact_id = int(tool_input["primary_contact_id"])
            matches = []
            page = 1
            pages = 1
            while page <= pages:
                result = call_pipeline_api("GET", f"/deals.json?conditions[company_id]={company_id}&per_page=200&page={page}")
                if result["status"] != 200:
                    logger.warning(f"create_deal: duplicate check got HTTP {result['status']}, proceeding with creation")
                    matches = []
                    break
                data = result["data"]
                pages = data.get("pagination", {}).get("pages", 1)
                for d in data.get("deals", data.get("data", [])):
                    if d.get("primary_contact_id") != primary_contact_id:
                        continue
                    if d.get("is_archived"):
                        continue
                    if d.get("deal_stage_id") == 2348038:  # Obsolete
                        continue
                    matches.append(d)
                page += 1
            if matches:
                existing = matches[0]
                existing_name = existing.get("name")
                existing_stage = (existing.get("deal_stage") or {}).get("name")
                existing_id = existing.get("id")
                return {
                    "error": "DUPLICATE_DEAL",
                    "message": f"A deal already exists for this person and company: '{existing_name}' (deal_id {existing_id}, stage {existing_stage}). Do NOT create another deal. Use update_deal on the existing deal if the terms changed.",
                    "existing_deal_id": existing_id,
                    "existing_deal_name": existing_name,
                    "existing_deal_stage": existing_stage,
                    "url": f"https://app.pipelinecrm.com/deals/{existing_id}"
                }
        except Exception as e:
            logger.warning(f"create_deal: duplicate check failed, proceeding with creation: {e}")

        type_map      = _DEAL_FIELDS["deal_type"]["values"]
        structure_map = _DEAL_FIELDS["structure"]["values"]
        class_map     = _DEAL_FIELDS["class"]["values"]
        nexus_map     = _DEAL_FIELDS["nexus"]["values"]
        series_map    = _DEAL_FIELDS["series"]["values"]
        layers_map    = _DEAL_FIELDS["layers"]["values"]

        custom_fields = {}
        # Required: deal type — case-insensitive lookup
        deal_type = tool_input.get("deal_type", "").lower()
        type_map_lower = {k.lower(): v for k, v in type_map.items()}
        if deal_type in type_map_lower:
            custom_fields["custom_label_1958"] = [type_map_lower[deal_type]]
        else:
            logger.error(f"create_deal: unrecognised deal_type '{deal_type}', type_map keys: {list(type_map.keys())}")
        # Optional fields
        if tool_input.get("gross"):
            custom_fields["custom_label_3064339"] = float(tool_input["gross"])
        if tool_input.get("net"):
            custom_fields["custom_label_3064369"] = float(tool_input["net"])
        if tool_input.get("min_size"):
            custom_fields["custom_label_3065488"] = float(tool_input["min_size"])
        if tool_input.get("max_size"):
            custom_fields["custom_label_3064645"] = float(tool_input["max_size"])
        raw_structure = tool_input.get("structure")
        struct_vals = raw_structure if isinstance(raw_structure, list) else ([raw_structure] if raw_structure else [])
        struct_ids = [structure_map[s] for s in struct_vals if s in structure_map]
        if struct_ids:
            custom_fields["custom_label_3064360"] = struct_ids
        if tool_input.get("series") in series_map:
            custom_fields["custom_label_3064333"] = series_map[tool_input["series"]]
        if tool_input.get("class") in class_map:
            custom_fields["custom_label_3064330"] = class_map[tool_input["class"]]
        if tool_input.get("nexus") in nexus_map:
            custom_fields["custom_label_3751449"] = nexus_map[tool_input["nexus"]]
        if tool_input.get("management_fee") is not None:
            custom_fields["custom_label_3940558"] = float(tool_input["management_fee"])
        if tool_input.get("carry") is not None:
            custom_fields["custom_label_3940559"] = float(tool_input["carry"])
        if tool_input.get("seller_fee") is not None:
            custom_fields["custom_label_3940560"] = float(tool_input["seller_fee"])
        if tool_input.get("partner_fee") is not None:
            custom_fields["custom_label_3940561"] = float(tool_input["partner_fee"])
        if tool_input.get("layers") in layers_map:
            custom_fields["custom_label_3938743"] = layers_map[tool_input["layers"]]
        if tool_input.get("num_shares"):
            custom_fields["custom_label_3070843"] = float(tool_input["num_shares"])
        custom_fields["custom_label_3994687"] = float(tool_input["refresh"]) if tool_input.get("refresh") is not None else 0.0

        # Auto-set CP Direct and Nexus from primary contact's Role, unless explicitly provided
        nexus_explicit = tool_input.get("nexus") in nexus_map
        cp_direct_explicit = "cp_direct" in tool_input
        if not (nexus_explicit and cp_direct_explicit):
            role = None
            try:
                contact = call_pipeline_api("GET", f"/people/{tool_input['primary_contact_id']}.json")
                if contact["status"] == 200:
                    role_val = (contact["data"].get("custom_fields") or {}).get("custom_label_3075382")
                    role = role_val[0] if isinstance(role_val, list) and role_val else role_val
            except Exception as e:
                logger.warning(f"create_deal: failed to fetch role for contact {tool_input.get('primary_contact_id')}: {e}")
            if role == 6438705:  # Intermediary
                if not cp_direct_explicit:
                    custom_fields["custom_label_3065662"] = 5080987  # No
                if not nexus_explicit:
                    custom_fields["custom_label_3751449"] = 6460635  # Co-Broker
            else:  # Investor (6596061) or blank/unset
                if not cp_direct_explicit:
                    custom_fields["custom_label_3065662"] = 5080984  # Yes
                if not nexus_explicit:
                    custom_fields["custom_label_3751449"] = 6460632  # Direct

        custom_fields["custom_label_4001285"] = 7187011  # Messaging = Disallow

        # Deal name is built here from company + sizes so the format holds regardless of what the model passed.
        # Company name comes from the live record (new companies may not be in the hourly snapshot);
        # fall back to the text before ':' in the model-supplied name.
        company_name = None
        try:
            company = call_pipeline_api("GET", f"/companies/{tool_input['company_id']}.json")
            if company["status"] == 200:
                company_name = (company["data"].get("name") or "").strip() or None
        except Exception as e:
            logger.warning(f"create_deal: failed to fetch company {tool_input.get('company_id')} for deal name: {e}")
        if not company_name:
            company_name = (tool_input.get("name") or "").split(":")[0].strip() or "Unknown"
        deal_name = build_deal_name(company_name, tool_input.get("min_size"), tool_input.get("max_size"), deal_type)

        # Web offer/bid: Structure comes from the submitter's form, never from names or org types.
        structure_source, structure_notice = None, None
        offer = _parse_web_offer((_CURRENT_EMAIL or {}).get("subject"), (_CURRENT_EMAIL or {}).get("body"))
        if _offer_applies(offer, tool_input["company_id"], company_name, deal_type):
            struct_vals_offer, structure_source, structure_notice = _offer_structure(offer)
            if sorted(struct_vals_offer) != sorted(struct_vals):
                logger.info(f"create_deal: structure {struct_vals} replaced by web offer value {struct_vals_offer}")
            struct_vals = struct_vals_offer
            custom_fields["custom_label_3064360"] = [structure_map[s] for s in struct_vals if s in structure_map]

        payload = {"deal": {
            "name": deal_name,
            "company_id": tool_input["company_id"],
            "primary_contact_id": tool_input["primary_contact_id"],
            "deal_stage_id": tool_input["deal_stage_id"],
            "revenue_type_id": 1027,  # Commission
            "custom_fields": custom_fields
        }}
        result = call_pipeline_api("POST", "/deals.json", payload)
        if result["status"] == 200:
            deal_id = result["data"].get("id")
            return {
                "success": True,
                "deal_id": deal_id,
                "deal_url": f"https://app.pipelinecrm.com/deals/{deal_id}",
                "deal_name": deal_name,
                "structure": struct_vals,
                **({"structure_source": structure_source} if structure_source else {}),
                **({"structure_notice": structure_notice} if structure_notice else {}),
                "message": "Deal created successfully."
            }
        return {"error": f"HTTP {result['status']}", "detail": result.get("data")}

    elif tool_name == "update_deal":
        deal_id = tool_input["deal_id"]
        fields = tool_input.get("fields", {})
        # Map human-readable deal fields to their custom_label equivalents
        DEAL_FIELD_MAP = {
            "management_fee": "custom_label_3940558",
            "carry":          "custom_label_3940559",
            "seller_fee":     "custom_label_3940560",
            "partner_fee":    "custom_label_3940561",
            "gross":          "custom_label_3064339",
            "net":            "custom_label_3064369",
            "min_size":       "custom_label_3065488",
            "max_size":       "custom_label_3064645",
            "num_shares":     "custom_label_3070843",
        }
        DEAL_ENUM_MAP = {
            "deal_type": _DEAL_FIELDS["deal_type"]["values"],
            "structure": _DEAL_FIELDS["structure"]["values"],
            "class":     _DEAL_FIELDS["class"]["values"],
            "nexus":     _DEAL_FIELDS["nexus"]["values"],
            "series":    _DEAL_FIELDS["series"]["values"],
            "layers":    _DEAL_FIELDS["layers"]["values"],
        }
        standard = {}
        custom = {}
        for k, v in fields.items():
            if k.startswith("custom_label_"):
                custom[k] = v
            elif k in DEAL_FIELD_MAP:
                if v is None:
                    custom[DEAL_FIELD_MAP[k]] = 0  # send 0 to clear
                else:
                    custom[DEAL_FIELD_MAP[k]] = float(v)
            elif k in DEAL_ENUM_MAP:
                label = {"deal_type": "custom_label_1958", "structure": "custom_label_3064360",
                         "class": "custom_label_3064330", "nexus": "custom_label_3751449",
                         "series": "custom_label_3064333", "layers": "custom_label_3938743"}[k]
                def _map_enum(val):
                    return (DEAL_ENUM_MAP[k].get(str(val)) or DEAL_ENUM_MAP[k].get(str(val).lower())
                            or {i.lower(): j for i, j in DEAL_ENUM_MAP[k].items()}.get(str(val).lower()))
                if k == "structure":
                    # multi-select: the full list sent replaces the deal's structure set
                    vals = v if isinstance(v, list) else [v]
                    ids = [m for m in (_map_enum(x) for x in vals) if m]
                    if ids:
                        custom[label] = ids
                else:
                    mapped = _map_enum(v)
                    if mapped:
                        custom[label] = [mapped] if k == "deal_type" else mapped
            else:
                standard[k] = v
        # Track which fields were intentionally cleared (set to None -> 0)
        cleared_fields = {k: DEAL_FIELD_MAP[k] for k, v in fields.items() if k in DEAL_FIELD_MAP and v is None}
        payload = {"deal": standard}
        if custom:
            payload["deal"]["custom_fields"] = custom
        result = call_pipeline_api("PUT", f"/deals/{deal_id}.json", payload)
        if result["status"] != 200:
            return {"error": f"HTTP {result['status']}", "detail": result.get("data")}
        if cleared_fields:
            verify = call_pipeline_api("GET", f"/deals/{deal_id}.json")
            if verify["status"] == 200:
                cf = verify.get("data", {}).get("custom_fields", {})
                failed = [k for k, label in cleared_fields.items() if cf.get(label) not in (None, "", 0)]
                if failed:
                    return {"success": False, "warning": f"Pipeline did not clear: {', '.join(failed)}. Clear manually in Pipeline."}
        return {"success": True, "message": "Deal updated successfully."}

    elif tool_name == "search_deals_cache":
        try:
            deals = get_deals_cache()
        except Exception as e:
            return {"error": f"Could not load deals cache: {str(e)}"}

        company = (tool_input.get("company") or "").lower()
        deal_type = (tool_input.get("deal_type") or "").lower()
        highlighted_only = tool_input.get("highlighted_only", False)
        country = (tool_input.get("country") or "").lower()
        max_size = tool_input.get("max_size")
        min_size = tool_input.get("min_size")
        structure = (tool_input.get("structure") or "").lower()
        series = (tool_input.get("series") or "").lower()
        limit = tool_input.get("limit", 20)

        matches = []
        for deal in deals:
            if company and company not in (deal.get("company") or "").lower():
                continue
            if deal_type and deal_type not in (deal.get("type") or "").lower():
                continue
            if highlighted_only and not deal.get("highlighted"):
                continue
            if country and country not in (deal.get("company_country") or "").lower():
                continue
            if structure and structure not in (deal.get("structure") or "").lower():
                continue
            if series and series.upper() != (deal.get("series") or "").upper():
                continue
            # Size filters — use max_size field
            deal_max = deal.get("max_size")
            deal_min = deal.get("min_size")
            try:
                if max_size and deal_min and float(deal_min) > float(max_size):
                    continue
                if min_size and deal_max and float(deal_max) < float(min_size):
                    continue
            except (ValueError, TypeError):
                pass
            matches.append(deal)

        # Sort: highlighted first, then by company name
        matches.sort(key=lambda d: (not d.get("highlighted"), d.get("company", "")))

        return {
            "total": len(matches),
            "showing": min(len(matches), limit),
            "deals": matches[:limit]
        }

    elif tool_name == "search_leads_cache":
        # Now reads from full snapshot instead of legacy cache
        security = tool_input["security_name"].lower().strip()
        interest_type = tool_input["interest_type"].lower().strip()
        # Map interest type to custom field key
        field_map = {
            "holding": "custom_label_3740611", "hold": "custom_label_3740611",
            "buying": "custom_label_3322093", "buy": "custom_label_3322093",
            "selling": "custom_label_3759156", "sell": "custom_label_3759156"
        }
        field_key = field_map.get(interest_type)
        if not field_key:
            return {"error": f"Unknown interest_type: {interest_type}"}
        # Find matching security entry IDs from SECURITY_IDS
        matching_ids = set()
        for sec_name, ids in SECURITY_IDS.items():
            if security in sec_name.lower():
                for v in ids.values():
                    if v:
                        matching_ids.add(v)
        if not matching_ids:
            return {"total": 0, "leads": [], "message": f"Security '{tool_input['security_name']}' not found"}
        people = get_snapshot("people.json")
        matches = []
        for p in people:
            cf = p.get("custom_fields", {})
            val = cf.get(field_key, [])
            ids = val if isinstance(val, list) else [val] if val else []
            person_ids = {int(x) for x in ids if x}
            if person_ids & matching_ids:
                matches.append({
                    "name": f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                    "company": p.get("company_name", ""),
                    "email": p.get("email", ""),
                    "id": p.get("id")
                })
        return {"total": len(matches), "leads": matches}

    elif tool_name == "ensure_client_folder":
        result = ensure_client_folder(
            tool_input.get("client_name", ""),
            tool_input.get("client_type", ""),
            tool_input.get("create_if_missing", True),
            bool(tool_input.get("create_new", False)),
            folder_id=tool_input.get("folder_id"),
            attachments=list(_CURRENT_ATTACHMENTS),
            close_match_review=tool_input.get("close_match_review"),
        )
        logger.info(f"ensure_client_folder: {json.dumps(result)}")
        return result

    elif tool_name == "upload_to_client_folder":
        result = upload_to_client_folder(
            tool_input.get("client_name", ""),
            tool_input.get("client_type", ""),
            tool_input.get("attachment_indexes") or None,
            tool_input.get("create_if_missing", True),
            bool(tool_input.get("create_new", False)),
            folder_id=tool_input.get("folder_id"),
            close_match_review=tool_input.get("close_match_review"),
        )
        logger.info(f"upload_to_client_folder: {json.dumps(result, default=str)}")
        return result

    elif tool_name == "rename_drive_file":
        result = rename_drive_file(
            tool_input.get("new_name", ""),
            file_id=tool_input.get("file_id"),
            current_name=tool_input.get("current_name"),
            client_name=tool_input.get("client_name"),
            client_type=tool_input.get("client_type"),
        )
        logger.info(f"rename_drive_file: {json.dumps(result, default=str)}")
        return result

    return {"error": f"Unknown tool: {tool_name}"}

# ── Agentic loop ──────────────────────────────────────────────────────────────
# Bedrock capacity errors (not code bugs): retried with backoff, then the next
# model ID is tried. Only IDs verified to exist in this account belong here.
BEDROCK_MODEL_IDS = ["us.anthropic.claude-sonnet-4-6"]
BEDROCK_RETRYABLE = {"ServiceUnavailableException", "ThrottlingException", "ModelNotReadyException"}
BEDROCK_BACKOFF_SECONDS = (2, 4, 8, 16)
BEDROCK_MAX_EXTRA_WAIT = 55
BEDROCK_OVERLOADED_MSG = "Bedrock was temporarily overloaded. Please resend your request in a few minutes."


class BedrockUnavailableError(Exception):
    """Every model ID in BEDROCK_MODEL_IDS stayed unavailable after backoff."""


def _converse_with_fallback(bedrock, **kwargs):
    """bedrock.converse over BEDROCK_MODEL_IDS. Capacity errors back off ~2/4/8/16s
    (+jitter, total extra wait capped at BEDROCK_MAX_EXTRA_WAIT across all models),
    then fall through to the next ID; an ID this account can't use is skipped.
    Returns (response, model_id)."""
    waited = 0.0
    last_err = None
    for model_id in BEDROCK_MODEL_IDS:
        for attempt in range(len(BEDROCK_BACKOFF_SECONDS) + 1):
            try:
                return bedrock.converse(modelId=model_id, **kwargs), model_id
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                last_err = e
                if code in ("AccessDeniedException", "ResourceNotFoundException", "ValidationException") \
                        and model_id != BEDROCK_MODEL_IDS[0]:
                    logger.warning(f"Bedrock fallback {model_id} unusable ({code}); skipping")
                    break
                if code not in BEDROCK_RETRYABLE:
                    raise
                if attempt == len(BEDROCK_BACKOFF_SECONDS):
                    logger.warning(f"Bedrock {model_id} still unavailable ({code}); trying next model")
                    break
                delay = BEDROCK_BACKOFF_SECONDS[attempt] * random.uniform(0.75, 1.25)
                delay = min(delay, BEDROCK_MAX_EXTRA_WAIT - waited)
                if delay <= 0:
                    logger.warning(f"Bedrock {model_id} unavailable ({code}); backoff budget spent")
                    break
                logger.warning(f"Bedrock {model_id} {code}; retry {attempt + 1} in {delay:.1f}s")
                time.sleep(delay)
                waited += delay
    raise BedrockUnavailableError(str(last_err))


def run_agent(instruction, attachments=None):
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1', config=Config(
        retries={"max_attempts": 10, "mode": "adaptive"}, read_timeout=300, connect_timeout=10))

    system_prompt = _SYSTEM_PROMPT

    # Build initial message content — text first, then any PDF attachments
    content = [{"text": instruction}]
    if attachments:
        for attachment in attachments:
            content.append({
                "document": {
                    "name": attachment["filename"],
                    "format": "pdf",
                    "source": {
                        "bytes": attachment["data"]
                    }
                }
            })

    messages = [{"role": "user", "content": content}]
    write_operations = []

    for iteration in range(12):
        logger.info(f"Agent iteration {iteration + 1}")

        response, model_id = _converse_with_fallback(
            bedrock,
            system=[{"text": system_prompt}],
            messages=messages,
            toolConfig={"tools": TOOL_SPECS},
            inferenceConfig={"maxTokens": 8000}
        )
        logger.info(f"Iteration {iteration + 1} answered by {model_id}")

        stop_reason = response["stopReason"]
        assistant_message = response["output"]["message"]
        messages.append(assistant_message)
        logger.info(f"Stop reason: {stop_reason}")

        if stop_reason == "end_turn":
            for block in assistant_message["content"]:
                if "text" in block:
                    return block["text"], write_operations
            return "Done.", write_operations

        elif stop_reason == "tool_use":
            tool_results = []
            for block in assistant_message["content"]:
                if "toolUse" in block:
                    tool_use = block["toolUse"]
                    tool_name = tool_use["name"]
                    tool_input = tool_use["input"]
                    tool_use_id = tool_use["toolUseId"]

                    if tool_name in ["update_person", "update_company", "add_note"]:
                        write_operations.append({"tool": tool_name, "input": tool_input})

                    result = execute_tool(tool_name, tool_input)
                    tool_results.append({
                        "toolResult": {
                            "toolUseId": tool_use_id,
                            "content": [{"json": {"result": result}}]
                        }
                    })

            messages.append({"role": "user", "content": tool_results})

        else:
            break

    return "I wasn't able to complete this task. Please try rephrasing.", write_operations

# ── Send reply email via SES ──────────────────────────────────────────────────
def send_reply(to_address, subject, body_text):
    ses = boto3.client('ses', region_name='us-east-1')
    ses.send_email(
        Source='agent@agent.graciagroup.com',
        Destination={'ToAddresses': [to_address]},
        ReplyToAddresses=['agent@agent.graciagroup.com'],
        Message={
            'Subject': {'Data': f"Re: {subject}"},
            'Body': {'Text': {'Data': body_text}}
        }
    )

# ── Main handler ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    global _CURRENT_ATTACHMENTS, _CURRENT_EMAIL
    _CURRENT_ATTACHMENTS = []
    _CURRENT_EMAIL = {}

    # Manual test invocation
    if 'instruction' in event:
        _CURRENT_EMAIL = {"subject": "", "body": event['instruction']}
        try:
            answer, write_ops = run_agent(event['instruction'])
            return {
                'statusCode': 200,
                'body': json.dumps({'answer': answer, 'write_operations': write_ops}, indent=2)
            }
        except Exception as e:
            import traceback
            return {'statusCode': 500, 'body': traceback.format_exc()}

    # SES email trigger
    try:
        ses_record = event['Records'][0]['ses']
        subject = ses_record['mail']['commonHeaders'].get('subject', '')
        from_list = ses_record['mail']['commonHeaders'].get('from', [''])
        from_address = from_list[0] if from_list else ''
        if '<' in from_address:
            from_address = from_address.split('<')[1].rstrip('>')

        import email as email_lib
        import email.header
        message_id = ses_record['mail']['messageId']
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket='gracia-agent-inbox', Key=message_id)
        raw_email = obj['Body'].read()

        # Parse email body and PDF attachments (bytes parser so attachment payloads stay byte-exact)
        msg = email_lib.message_from_bytes(raw_email)
        text_body = ''
        attachments = []
        raw_attachments = []
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                raw_name = part.get_filename()
                if raw_name:
                    part_bytes = part.get_payload(decode=True) or b''
                    is_inline_logo = (content_type.startswith('image/') and part.get('Content-ID')
                                      and len(part_bytes) < 100 * 1024)
                    if part_bytes and not is_inline_logo:
                        try:
                            orig_name = str(email_lib.header.make_header(email_lib.header.decode_header(raw_name)))
                        except Exception:
                            orig_name = raw_name
                        raw_attachments.append({"index": len(raw_attachments) + 1, "filename": _clean_filename(orig_name),
                                                "mime": content_type, "data": part_bytes, "size": len(part_bytes)})
                if content_type == 'text/plain' and not text_body:
                    payload = part.get_payload(decode=True)
                    if payload:
                        text_body = payload.decode('utf-8', errors='replace')
                elif content_type == 'application/pdf':
                    pdf_bytes = part.get_payload(decode=True)
                    if pdf_bytes:
                        filename = part.get_filename() or 'attachment.pdf'
                        # Strip extension, replace underscores/dots with spaces, then keep only Bedrock-allowed chars
                        filename = re.sub(r'\.[^.]+$', '', filename)          # remove extension
                        filename = re.sub(r'[_.]', ' ', filename)              # underscores/dots → spaces
                        filename = re.sub(r'[^a-zA-Z0-9\s\-\(\)\[\]]', '', filename).strip()  # remove remaining invalid chars
                        filename = re.sub(r'\s{2,}', ' ', filename) or 'attachment'  # collapse spaces
                        attachments.append({"filename": filename, "data": pdf_bytes})
                        logger.info(f"PDF attachment found: {filename} ({len(pdf_bytes)} bytes)")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                text_body = payload.decode('utf-8', errors='replace')
            else:
                text_body = msg.get_payload() or ''

        # ── SNS new user signup handler ──────────────────────────────────────────
        if 'sns.amazonaws.com' in from_address.lower() or 'amazonses.com' in from_address.lower():
            # Check for signup notification pattern
            import re as _re
            signup_match = _re.search(r'New user signup:\s*([\w.+%-]+@[\w.-]+\.\w+)', text_body)
            if signup_match:
                signup_email = signup_match.group(1).strip().lower()
                logger.info(f"SNS signup detected: {signup_email}")

                # Hard duplicate check in Python before invoking agent
                existing_check = call_pipeline_api("GET", f"/people.json?conditions[email]={urllib.parse.quote(signup_email)}&per_page=3")
                existing_person = None
                if existing_check["status"] == 200:
                    for entry in existing_check["data"].get("entries", []):
                        if (entry.get("email") or "").lower().strip() == signup_email:
                            existing_person = entry
                            break

                if not existing_person:
                    people = get_snapshot("people.json")
                    for p in people:
                        if (p.get("email") or "").lower().strip() == signup_email.lower().strip():
                            existing_person = p
                            break

                if existing_person:
                    pid = existing_person["id"]
                    pname = f"{existing_person.get('first_name','')} {existing_person.get('last_name','')}".strip()
                    logger.info(f"SNS signup: existing person found {pid} ({pname})")
                    # Update AWS Reg and ensure newsletter is set
                    call_pipeline_api("PUT", f"/people/{pid}.json", {"person": {
                        "custom_fields": {"custom_label_3954543": "7043466"}
                    }})
                    # Check newsletter
                    cf = existing_person.get("custom_fields", {})
                    newsletter = cf.get("custom_label_3775335")
                    if not newsletter:
                        call_pipeline_api("PUT", f"/people/{pid}.json", {"person": {
                            "custom_fields": {"custom_label_3775335": 6613673}
                        }})
                    answer = f"Existing lead found: {pname} ({signup_email})\nPipeline: https://app.pipelinecrm.com/people/{pid}\nAWS Reg set to Active. Newsletter was {'already set' if newsletter else 'set to Confirm'}."
                else:
                    logger.info(f"SNS signup: new person, invoking agent for {signup_email}")
                    answer, _ = run_agent(
                        f"AWS SIGNUP AUTOMATION - DO NOT ASK CHAD FOR CONFIRMATION - PROCEED AUTOMATICALLY:\n\n"
                        f"A new user just signed up at trades.graciagroup.com with email: {signup_email}\n\n"
                        f"This person does NOT exist in Pipeline (already verified). Create a new lead:\n"
                        f"- source_id=3581824 (AWS)\n"
                        f"- custom_label_3954543=[7043466] (AWS Reg=Active)\n"
                        f"- Newsletter=Confirm (6613673)\n"
                        f"- Whitelist tag (3280123), Role=Investor (6596061), Investor Level=Substantive (7162165)\n"
                        f"- Research the person via web_search and fetch_webpage, set Transactor Type, city, summary\n"
                        f"- Return the Pipeline URL"
                    )
                # Send alert email to Chad
                ses_client = boto3.client('ses', region_name='us-east-1')
                ses_client.send_email(
                    Source='agent@agent.graciagroup.com',
                    Destination={'ToAddresses': ['cgracia@rainmakersecurities.com']},
                    ReplyToAddresses=['agent@agent.graciagroup.com'],
                    Message={
                        'Subject': {'Data': f'New signup: {signup_email}'},
                        'Body': {'Text': {'Data': answer}}
                    }
                )
                logger.info(f"SNS signup handled for {signup_email}")
                return {'statusCode': 200, 'body': 'SNS signup handled'}
            return {'statusCode': 200, 'body': 'SNS non-signup ignored'}

        # Only process emails from Chad's addresses
        allowed = ['cgracia@graciagroup.com', 'cgracia@rainmakersecurities.com', 'chad@graciagroup.com', 'kate@graciagroup.com']
        if not any(a in from_address.lower() for a in allowed):
            logger.info(f"Ignoring email from non-allowed sender: {from_address}")
            return {'statusCode': 200, 'body': 'Ignored'}

        # Read full email from S3
        # Include To/CC recipients so agent knows who Chad is emailing in bcc mode
        to_list = ses_record['mail']['commonHeaders'].get('to', [])
        cc_list = ses_record['mail']['commonHeaders'].get('cc', [])
        recipients = ", ".join(to_list + cc_list)
        bcc_context = f"[Chad sent this to: {recipients}]\n\n" if recipients else ""
        instruction = f"Subject: {subject}\n\n{bcc_context}{text_body.strip()}" if text_body.strip() else subject
        _CURRENT_ATTACHMENTS = raw_attachments
        _CURRENT_EMAIL = {"from": from_address, "subject": subject, "body": text_body}
        if raw_attachments:
            inventory = "; ".join(f"{a['index']}) {a['filename']} ({a['mime']}, {_fmt_size(a['size'])})"
                                  for a in raw_attachments)
            instruction = f"[Attachments on this email: {inventory}]\n\n{instruction}"
            logger.info(f"Raw attachments: {[(a['index'], a['filename'], a['mime'], a['size']) for a in raw_attachments]}")
        logger.info(f"Email from: {from_address}, subject: {subject}, body length: {len(text_body)}, attachments: {len(attachments)}")

    except (KeyError, IndexError) as e:
        logger.error(f"Error parsing event: {e}")
        return {'statusCode': 400, 'body': 'Invalid event'}

    try:
        answer, write_ops = run_agent(instruction, attachments=attachments if attachments else None)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        try:
            send_reply(from_address, subject, BEDROCK_OVERLOADED_MSG if isinstance(e, BedrockUnavailableError)
                       else f"Sorry, I encountered an error: {str(e)}")
        except:
            pass
        return {'statusCode': 500, 'body': str(e)}

    reply = answer

    try:
        send_reply(from_address, subject, reply)
    except Exception as e:
        logger.error(f"SES error: {e}")

    return {'statusCode': 200, 'body': 'OK'}
