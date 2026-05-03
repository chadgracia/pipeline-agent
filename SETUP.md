# Pipeline Agent — Work Setup Overview

This document is the orientation guide for a Claude session (or human) coming into this project cold. It describes what the agent is, how it runs in AWS, what GitHub repo holds the code, and how the pieces fit together.

## What this project is

`pipeline-agent` is an AI assistant that automates Chad's CRM workflows in **Pipeline CRM** (api.pipelinecrm.com). It runs as a single AWS Lambda function and is invoked two ways:

1. **By inbound email** (SES) — Chad emails the agent (or BCCs it) and it acts on the email contents, including PDF attachments.
2. **By SNS signup notifications** — when a new user signs up at `trades.graciagroup.com`, SNS forwards the notice and the agent creates/updates a lead in Pipeline.

Under the hood it's a tool-using Claude agent (Bedrock Converse API) with ~20+ Pipeline CRM tools (search/create/update people, companies, deals), web search, webpage fetching, and PDF parsing.

## Repository

- **GitHub:** `chadgracia/pipeline-agent`
- **Default branch:** `main`
- **Layout:**
  - `lambda_function.py` — the entire Lambda. One file, ~1600 lines. Contains the handler, the Bedrock agent loop, all tool implementations, and the SES/SNS routing logic.
  - `README.md` — minimal placeholder.
  - `.gitignore` — Python build artifacts.
- **Deployment:** code in this repo is the source of truth for the Lambda. There is no CI/CD wired up here — deployment to AWS is done manually (zip + upload, or paste into the console). If you change `lambda_function.py`, you need to push the change to AWS Lambda separately from pushing to GitHub.

## AWS resources

All resources live in `us-east-1`.

### Lambda
- One function whose code is `lambda_function.py`.
- Entry point: `lambda_handler(event, context)`.
- Three event shapes are handled:
  - `{"instruction": "..."}` — manual test invocation from the console.
  - SES inbound email record (`event["Records"][0]["ses"]`).
  - SNS-forwarded signup notifications (still arrive as SES records, detected by the `sns.amazonaws.com` / `amazonses.com` sender).

### S3 buckets
The Lambda reads from four buckets. Nothing is written by the agent except via the Pipeline CRM API; S3 is read-only configuration + cache.

| Bucket | Key(s) | Purpose |
|---|---|---|
| `pipeline-token` | `pipeline-jwt.json` | Pipeline CRM JWT (rotate here, not in code) |
| `pipeline-token` | `agent-data.json` | Security IDs and deal field metadata loaded at cold start |
| `pipeline-token` | `agent-system-prompt.txt` | The agent's system prompt — edit here to change behavior |
| `fetched-leads` | `leads_data.json`, `deals_data.json` | Cached lead/deal lists |
| `full-pipeline-cache` | `people.json`, `companies.json`, `deals.json` | Full Pipeline snapshots used as fallback search |
| `gracia-agent-inbox` | `<SES messageId>` | Raw inbound emails dropped by SES rule |

### SES (Simple Email Service)
- **Inbound:** SES receive rule writes incoming mail to `s3://gracia-agent-inbox/<messageId>` and triggers the Lambda.
- **Allowed senders** (hard-coded in `lambda_handler`): `cgracia@graciagroup.com`, `cgracia@rainmakersecurities.com`, `chad@graciagroup.com`, `kate@graciagroup.com`. Anything else is dropped.
- **Outbound:** the agent sends replies / alerts as `agent@agent.graciagroup.com`. Signup alerts go to `cgracia@rainmakersecurities.com`.

### SNS
- A topic from the `trades.graciagroup.com` signup flow forwards new-user emails into SES, which lands them in the same Lambda. Detected by sender domain and parsed with a regex (`New user signup: <email>`).

### Bedrock
- Model: `us.anthropic.claude-sonnet-4-6` via Bedrock Converse, region `us-east-1`.
- The Lambda's IAM role needs `bedrock:InvokeModel` (or the Converse equivalent) and read access to the S3 buckets above and SES send.

## External integrations

- **Pipeline CRM** — REST API at `https://api.pipelinecrm.com/api/v3`, auth via JWT loaded from S3 each cold start. All writes (create/update people, companies, deals) go through `call_pipeline_api`.
- **DuckDuckGo HTML** — used for the `web_search` tool (no API key).
- **Generic HTTP fetch** — `fetch_webpage` tool for company research.

## Custom field / ID conventions baked into the code

These are Pipeline CRM-specific magic numbers worth knowing about:

- `source_id=3581824` → AWS signup source.
- `custom_label_3954543=7043466` → "AWS Reg = Active".
- `custom_label_3775335=6613673` → "Newsletter = Confirm".
- Tag `3280123` → Whitelist.
- Role `6596061` → Investor; Investor Level `7162165` → Substantive.
- Industry name → ID mapping is the `INDUSTRY_MAP` dict near the top of `lambda_function.py`.

If a field ID changes in Pipeline, it has to change here too.

## Local development

- This is a plain Python file; there is no `requirements.txt` because the only third-party dep is `boto3`, which is provided by the Lambda runtime.
- You generally won't run it locally — testing is done with `{"instruction": "..."}` manual invocations in the Lambda console, or by emailing the agent.
- The `claude/document-setup-overview-bfpqn` branch (and similar `claude/*` branches) are workspaces for Claude-driven changes; they merge into `main` once reviewed.

## Should the GitHub repo be private or public?

**Recommendation: keep it private.**

Reasons:

1. **Operational logic is sensitive even without secrets.** The repo doesn't contain credentials (the JWT lives in S3, which is correct), but it does contain:
   - Internal email addresses and the allow-list of who can command the agent.
   - Pipeline CRM custom-field IDs, tag IDs, and source IDs that map to your sales process.
   - The exact automation rules for new-signup handling.
   These are reverse-engineerable into a picture of how Gracia Group / Rainmaker Securities runs deal flow. That's competitive information, not open-source value.

2. **No upside to being public.** Nobody else can run this — it's hard-wired to your Pipeline tenant, your S3 buckets, your SES identities, and your Bedrock account. There's no community, no reusable library, no portfolio benefit that requires public visibility.

3. **Attack surface.** A public repo invites people to read the SNS/SES flow looking for ways to spoof a signup or slip past the sender allow-list. It's a low-likelihood risk, but it's a free risk to eliminate by clicking "Private."

4. **You can still grant access.** GitHub private repos support per-collaborator access and Claude/Anthropic integrations work the same on private repos.

**When you'd flip it public:** only if you intentionally want to publish it as a reference example and you've first scrubbed the custom-field IDs, allow-listed emails, bucket names, and any signup-flow specifics into placeholders. That's a meaningful rewrite, not a one-click change.

## Quick checklist for a new Claude session

- Code lives in `lambda_function.py` on `chadgracia/pipeline-agent`.
- Develop on a `claude/*` branch, merge to `main`.
- Pushing to GitHub does **not** deploy — the Lambda has to be updated separately.
- System prompt is in S3 (`pipeline-token/agent-system-prompt.txt`), not in the repo.
- JWT is in S3 (`pipeline-token/pipeline-jwt.json`); never paste it into code.
- Region for everything AWS is `us-east-1`.
- Model is `us.anthropic.claude-sonnet-4-6` via Bedrock Converse.
