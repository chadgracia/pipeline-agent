import json
import boto3
import urllib.request
import urllib.error
import urllib.parse
import logging
import re

# ── Load agent data from S3 at cold start ─────────────────────────────────────
def _load_agent_data():
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket="pipeline-token", Key="agent-data.json")
    return json.loads(obj['Body'].read())

_AGENT_DATA     = _load_agent_data()
SECURITY_IDS    = _AGENT_DATA["security_ids"]
_DEAL_FIELDS    = _AGENT_DATA["deal_fields"]

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
                "phone": {"type": "string"},
                "mobile": {"type": "string"},
                "company_name": {"type": "string"},
                "position": {"type": "string"},
                "linked_in_url": {"type": "string"},
                "website": {"type": "string"},
                "summary": {"type": "string"},
                "work_city": {"type": "string"},
                "work_country": {"type": "string"},
                "custom_label_3075382": {"type": "integer", "description": "Role: 6596061=Investor, 6438705=Intermediary"},
                "custom_label_3759163": {"type": "integer", "description": "Transactor Type: 6484811=Family Office, 6484810=Natural Person, 6484812=Institution, 6484808=VC or PE Fund, 7037492=Hedge Fund, 6484813=Wealth Advisor, 6577160=Co-Broker, 6888332=Foreign Finder"},
                "custom_label_3923758": {"type": "integer", "description": "Investor Level: 7162165=Substantive (default), 6950564=Qualified Purchaser, 6950563=Accredited Investor, 6950561=Unknown"},
                "custom_label_3775335": {"type": "integer", "description": "Weekly Newsletter: 6613673=Confirm, 6613674=Subscribed"}
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
            "description": "Update a company record fields.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "company_id": {"type": "integer"},
                "fields": {"type": "object"}
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
                "last_round_valuation": {"type": "number", "description": "Last round valuation in whole dollars"},
                "last_round_date": {"type": "string", "description": "Last round date in YYYY-MM-DD format"},
                "website": {"type": "string", "description": "Company website"},
                "address": {"type": "string", "description": "Company address"},
                "legal_name": {"type": "string", "description": "Full legal name"}
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
            "description": "Create a new deal in Pipeline CRM. Always look up company_id via search_companies and primary_contact_id via search_people first. Deal name format: 'CompanyName: $XM Buy/Sell'. Revenue type is always Commission. Stage is Firm if price and size are known, Inquiry if not.",
            "inputSchema": {"json": {"type": "object", "properties": {
                "name": {"type": "string", "description": "Deal name e.g. 'SpaceX: $2M Sell'"},
                "company_id": {"type": "integer", "description": "Pipeline company ID — look up via search_companies first"},
                "primary_contact_id": {"type": "integer", "description": "Pipeline person ID — look up via search_people first"},
                "deal_stage_id": {"type": "integer", "description": "2109142=Inquiry, 111800=Firm, 2381534=Matched, 2388323=Confirm, 2094373=Hold. Use Firm if price AND size known, else Inquiry."},
                "deal_type": {"type": "string", "enum": ["buy", "sell"], "description": "buy or sell — required"},
                "gross": {"type": "number", "description": "Gross price per share"},
                "net": {"type": "number", "description": "Net price per share"},
                "min_size": {"type": "number", "description": "Minimum deal size in dollars"},
                "max_size": {"type": "number", "description": "Maximum deal size in dollars"},
                "structure": {"type": "string", "enum": ["Direct", "Fund", "Forward"], "description": "Deal structure"},
                "series": {"type": "string", "description": "Series e.g. A, B, C, D, E, F, Seed, N/A"},
                "class": {"type": "string", "enum": ["Common", "Preferred", "Mixed", "Any"], "description": "Share class"},
                "nexus": {"type": "string", "enum": ["Direct", "RMS Broker", "Co-Broker", "Foreign Finder"], "description": "How the deal came in"},
                "management_fee": {"type": "number", "description": "SPV management fee percentage"},
                "carry": {"type": "number", "description": "SPV carry percentage"},
                "seller_fee": {"type": "number", "description": "One-time upfront seller fee % (not annual). Use for 'upfront fee', 'one-time fee', or 'seller fee' mentions."},
                "partner_fee": {"type": "number", "description": "Fee % charged by a co-broker, foreign finder, or other intermediary that is NOT covered by our standard fee-sharing agreement. Use this when a third party is charging their own separate fee to the client that Chad does not share in."},
                "layers": {"type": "string", "enum": ["SPV on cap table", "2-Layer SPV", "3-Layer SPV"], "description": "SPV layer structure"},
                "num_shares": {"type": "number", "description": "Number of shares in the deal"}
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
    }
]

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
    structure = STRUCTURE_MAP.get(raw_structure[0] if isinstance(raw_structure, list) else raw_structure)

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

def _execute_tool_inner(tool_name, tool_input):

    if tool_name == "search_companies":
        query = tool_input['name'].lower().strip()
        companies = get_snapshot("companies.json")
        matches = [c for c in companies if query in (c.get("name") or "").lower()][:10]
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
            email = (p.get("email") or "").lower()
            name = f"{p.get('first_name','')} {p.get('last_name','')}".lower().strip()
            company = (p.get("company_name") or "").lower()
            if '@' in query:
                if query == email:
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
        people = get_snapshot("people.json")
        for p in people:
            if p.get("id") == pid:
                return p
        # Fallback to live API if not in snapshot
        result = call_pipeline_api("GET", f"/people/{pid}.json")
        return result["data"] if result["status"] == 200 else {"error": f"Person {pid} not found"}

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
            check = call_pipeline_api("GET", f"/people.json?conditions[email]={urllib.parse.quote(email)}&per_page=5")
            if check["status"] == 200 and check["data"].get("entries"):
                for existing in check["data"]["entries"]:
                    # Only block if email exactly matches the record's email field
                    if (existing.get("email") or "").lower().strip() == email.lower().strip():
                        return {
                            "duplicate": True,
                            "message": f"Person with email {email} already exists. Do NOT create a duplicate. Update the existing record instead.",
                            "existing_person_id": existing["id"],
                            "existing_name": f"{existing.get('first_name','')} {existing.get('last_name','')}".strip(),
                            "pipeline_url": f"https://app.pipelinecrm.com/people/{existing['id']}"
                        }
        # Build person payload from provided fields
        person_data = {}
        standard_fields = ["first_name", "last_name", "email", "phone", "mobile",
                           "company_name", "position", "linked_in_url", "website",
                           "summary", "work_city", "work_country", "work_state",
                           "work_street", "work_phone", "source_id"]
        custom_fields = {}
        for field, value in tool_input.items():
            if not value:
                continue
            if field in standard_fields:
                person_data[field] = value
            elif field.startswith("custom_label_"):
                custom_fields[field] = value
        # Always set type to Lead and add Whitelist tag
        person_data["type"] = "Lead"
        person_data["predefined_contacts_tag_ids"] = [3280123]
        result = call_pipeline_api("POST", "/people.json", {"person": person_data})
        if result["status"] == 200:
            new_id = result["data"].get("id")
            pipeline_url = f"https://app.pipelinecrm.com/people/{new_id}"
            # Step 2: update custom fields separately via PUT (more reliable than POST)
            if custom_fields:
                MULTI_SELECT_FIELDS = {"custom_label_3322093", "custom_label_3759156", "custom_label_3740611", "custom_label_3052210"}
                formatted_cf = {}
                for k, v in custom_fields.items():
                    if k in MULTI_SELECT_FIELDS and isinstance(v, list):
                        formatted_cf[k] = [int(x) for x in v if str(x).strip().isdigit()]
                    else:
                        formatted_cf[k] = v
                call_pipeline_api("PUT", f"/people/{new_id}.json", {"person": {"custom_fields": formatted_cf}})
            return {"success": True, "person_id": new_id, "pipeline_url": pipeline_url, "message": f"Created. Reply must include the pipeline_url so Chad can verify."}
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "update_person":
        # Split standard fields from custom fields
        standard = {}
        custom = {}
        for k, v in tool_input["fields"].items():
            if k.startswith("custom_label_"):
                custom[k] = v
            else:
                standard[k] = v
        # For multi-select fields, fetch existing and merge — send as JSON arrays
        MULTI_SELECT_FIELDS = {"custom_label_3322093", "custom_label_3759156", "custom_label_3740611", "custom_label_3052210"}
        if custom:
            existing = call_pipeline_api("GET", f"/people/{tool_input['person_id']}.json")
            if existing["status"] == 200:
                existing_cf = existing["data"].get("custom_fields", {})
                for k, v in list(custom.items()):
                    if k not in MULTI_SELECT_FIELDS:
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
        payload = {"person": standard}
        if custom:
            payload["person"]["custom_fields"] = custom
        result = call_pipeline_api("PUT", f"/people/{tool_input['person_id']}.json", payload)
        if result["status"] == 200:
            return {"success": True, "message": "Record updated. Do not verify or retry — report success to the user."}
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "update_company":
        standard = {}
        custom = {}
        for k, v in tool_input["fields"].items():
            if k.startswith("custom_label_"):
                custom[k] = v
            else:
                standard[k] = v
        # For multi-select fields, fetch existing and merge — send as JSON arrays
        COMPANY_MULTI_SELECT = {"custom_label_3749627", "custom_label_3749628", "custom_label_3746654"}
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
        result = call_pipeline_api("PUT", f"/companies/{tool_input['company_id']}.json", payload)
        if result["status"] == 200:
            return {"success": True, "message": "Record updated. Do not verify or retry — report success to the user."}
        return {"success": False, "status": result["status"], "error": result["data"]}

    elif tool_name == "create_company":
        company_data = {"name": tool_input["name"]}
        if tool_input.get("summary"):
            company_data["summary"] = tool_input["summary"]
        if tool_input.get("website"):
            company_data["website"] = tool_input["website"]
        if tool_input.get("address"):
            company_data["address"] = tool_input["address"]
        if tool_input.get("legal_name"):
            company_data["legal_name"] = tool_input["legal_name"]
        custom_fields = {}
        if tool_input.get("org_type"):
            custom_fields["custom_label_625142"] = tool_input["org_type"]
        if tool_input.get("last_round_pps") is not None:
            custom_fields["custom_label_3064363"] = tool_input["last_round_pps"]
        if tool_input.get("last_round_valuation") is not None:
            custom_fields["custom_label_3790429"] = tool_input["last_round_valuation"]
        if tool_input.get("last_round_date"):
            custom_fields["custom_label_3064364"] = tool_input["last_round_date"]
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
        def lookup_one(name):
            if name in SECURITY_IDS:
                return format_ids(name, SECURITY_IDS[name])
            name_lower = name.lower()
            for k, v in SECURITY_IDS.items():
                if k.lower() == name_lower:
                    return format_ids(k, v)
            matches = [(k, v) for k, v in SECURITY_IDS.items() if name_lower in k.lower()]
            if matches:
                return {"matches": [format_ids(k, v) for k, v in matches[:5]]}
            return {"error": f"Security '{name}' not found in lookup table"}
        names = tool_input.get("security_names")
        if names:
            return {"results": [lookup_one(n) for n in names]}
        return lookup_one(tool_input.get("security_name", ""))

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
        if tool_input.get("structure") in structure_map:
            custom_fields["custom_label_3064360"] = [structure_map[tool_input["structure"]]]
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

        payload = {"deal": {
            "name": tool_input["name"],
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
                mapped = DEAL_ENUM_MAP[k].get(str(v)) or DEAL_ENUM_MAP[k].get(str(v).lower()) or {i.lower(): j for i, j in DEAL_ENUM_MAP[k].items()}.get(str(v).lower())
                if mapped:
                    label = {"deal_type": "custom_label_1958", "structure": "custom_label_3064360",
                             "class": "custom_label_3064330", "nexus": "custom_label_3751449",
                             "series": "custom_label_3064333", "layers": "custom_label_3938743"}[k]
                    custom[label] = [mapped] if k in ("deal_type", "structure") else mapped
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

    return {"error": f"Unknown tool: {tool_name}"}

# ── Agentic loop ──────────────────────────────────────────────────────────────
def run_agent(instruction, attachments=None):
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

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

        response = bedrock.converse(
            modelId="us.anthropic.claude-sonnet-4-6",
            system=[{"text": system_prompt}],
            messages=messages,
            toolConfig={"tools": TOOL_SPECS},
            inferenceConfig={"maxTokens": 8000}
        )

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
        Source='cgracia@graciagroup.com',
        Destination={'ToAddresses': [to_address]},
        Message={
            'Subject': {'Data': f"Re: {subject}"},
            'Body': {'Text': {'Data': body_text}}
        }
    )

# ── Main handler ──────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    # Manual test invocation
    if 'instruction' in event:
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
        message_id = ses_record['mail']['messageId']
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket='gracia-agent-inbox', Key=message_id)
        raw_email = obj['Body'].read().decode('utf-8', errors='replace')

        # Parse email body and PDF attachments
        msg = email_lib.message_from_string(raw_email)
        text_body = ''
        attachments = []
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
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
                        if entry.get("email", "").lower().strip() == signup_email:
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
        logger.info(f"Email from: {from_address}, subject: {subject}, body length: {len(text_body)}, attachments: {len(attachments)}")

    except (KeyError, IndexError) as e:
        logger.error(f"Error parsing event: {e}")
        return {'statusCode': 400, 'body': 'Invalid event'}

    try:
        answer, write_ops = run_agent(instruction, attachments=attachments if attachments else None)
    except Exception as e:
        logger.error(f"Agent error: {e}")
        try:
            send_reply(from_address, subject, f"Sorry, I encountered an error: {str(e)}")
        except:
            pass
        return {'statusCode': 500, 'body': str(e)}

    reply = answer

    try:
        send_reply(from_address, subject, reply)
    except Exception as e:
        logger.error(f"SES error: {e}")

    return {'statusCode': 200, 'body': 'OK'}
