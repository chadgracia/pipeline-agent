# pipeline-agent — project memory

AWS Lambda that acts as a CRM assistant for Chad Gracia (Rainmaker Securities, FINRA-registered broker-dealer). Reads inbound SES email (direct instructions and BCCs), AWS Cognito SNS signup notifications, and a manual `instruction` test event; replies via SES; writes to Pipeline CRM via REST.

## Codebase shape
- `lambda_function.py` — the whole Lambda. Single file, ~1,800 lines. Entry point: `lambda_handler(event, context)` at the bottom.
- `agent-system-prompt.txt` — the system prompt that drives the agent's reasoning. **Source of truth for agent behavior** — most "the agent did X wrong" fixes belong here, not in code.
- `.github/workflows/sync-agent-prompt.yml` — on push to `main` touching `agent-system-prompt.txt`, syncs the file to `s3://pipeline-token/agent-system-prompt.txt`. Lambda reads it from there on every invocation.
- No test suite. No `requirements.txt` (Lambda runtime provides `boto3`).

## Runtime
- LLM: Amazon Bedrock, `us.anthropic.claude-sonnet-4-6` (`lambda_function.py:1559`).
- S3 bucket: `pipeline-token`. Holds `agent-system-prompt.txt`, `agent-data.json`, `pipeline-jwt.json`, and read-only snapshots `people.json`, `companies.json`, `deals.json`.
- Tool layer: `search_people`, `get_person`, `create_person`, `update_person`, `search_companies`, `create_deal`, `update_deal`, `get_security_ids`, etc. — defined in `lambda_function.py` around lines 200–500 (schemas) and 540+ (handlers).
- Snapshot vs live: most `search_*` / `get_*` tools read from S3 snapshots for speed; `create_*` / `update_*` always go through `call_pipeline_api` to Pipeline's REST API.

## Triggers
- **SES** — inbound email to `agent@agent.graciagroup.com`. Two patterns: direct instructions ("update Naji's email status...") and **BCC mode** (Chad bcc's the agent on a thread; the agent reads context and updates whatever's revealed). See RULE 1 in `agent-system-prompt.txt`.
- **SNS** — new-user signups from `trades.graciagroup.com` (AWS Cognito → SNS → Lambda). Triggers a duplicate check and lead creation.
- **Manual** — invoke with `{"instruction": "..."}` for testing; returns the agent's answer and the list of write operations it performed.

## Deploy
- **Prompt changes** (`agent-system-prompt.txt`): commit to `main` → GitHub Action syncs to S3 → live on next Lambda invocation. No manual step.
- **Code changes** (`lambda_function.py`): Deploys are automatic: push/merge to main triggers `.github/workflows/deploy.yml`, which zips `lambda_function.py` and runs `aws lambda update-function-code` on the pipeline-agent Lambda via the `github-actions-deploy` OIDC role. No manual release step.

## Pipeline CRM custom-field IDs (most-used)
- `custom_label_3796440` — CEF Submitted: 6600515=Yes, 6600514=Pending, 6600513=No, 6600516=N/A
- `custom_label_3763008` — IQF Submitted: 6496840=Yes, 6496842=Pending, 6496841=No, 6596073=Unnecessary
- `custom_label_2447206` — Email Status: 5141011=Functioning, 3940558=Bouncing, 4943722=Blocked, 3940678=Needed, 3948784=Unnecessary, 4781320=Unsubscribed, 4923706=In Progress
- `custom_label_3751449` — Nexus: 6460632=Direct, 6460633=RMS Broker, 6460635=Co-Broker, 6460634=Foreign Finder (also `NEXUS_MAP` in code)
- `custom_label_3075382` — Role: 6596061=Investor (default), 6438705=Intermediary
- Full list lives in `agent-system-prompt.txt` and in `lambda_function.py:475`+ (STRUCTURE_MAP, NEXUS_MAP, etc.).

## Conventions
- Branch names: `claude/<topic>-<id>` (e.g. `claude/email-comparison-null-handling-O0lVN`). The session-assigned branch is the only branch to push to without explicit permission.
- Commits: write the **why**, not the what. Squash on merge.
- Do not push to `main` directly. Open a PR.
- The S3 bucket name and prompt-sync workflow are committed convention — don't rename without coordinating an AWS-side rename.
- Person records have three email slots: `email` (primary work), `email2`, `home_email`. Lookup and dedup paths should check all three (the AWS-signup handler near `lambda_function.py:1674` is the one remaining primary-only check — flagged as a known gap).
- Pipeline CRM rejects numeric fields containing commas — always strip before sending.
- `custom_label_*__remove` is the convention for removing individual options from a multi-select in `update_person` / `update_deal`.
