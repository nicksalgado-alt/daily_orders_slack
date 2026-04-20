# daily_orders_slack

Cloud-hosted replacement for the local `nok-daily-orders-slack` Cowork task.
Runs in GitHub Actions every morning at ~7 AM ET, pulls the last 34 days of
orders from Extensiv Integration Manager, builds a brand × marketplace pivot
workbook, and posts a Slack summary with the xlsx attached to `#resale`.

No dependency on Nick's laptop being awake.

## Layout

```
daily_orders_slack/
├── .github/workflows/
│   └── daily.yml             # The cron job
├── references/
│   └── brand_map.json        # Brand-mapping rules (copied from extensiv-im skill)
├── scripts/
│   ├── client.py             # Extensiv IM HTTP client (copied from extensiv-im skill)
│   ├── brand_mapper.py       # Brand-mapping logic (copied from extensiv-im skill)
│   └── run_daily.py          # Orchestrator: pull → build → slack
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup

1. Add repo secrets at **Settings → Secrets and variables → Actions → New repository secret**:

   | Secret               | Value                                                     |
   | -------------------- | --------------------------------------------------------- |
   | `IM_MERCHANT_USER`   | `10160Nt0SwJof`                                           |
   | `IM_MERCHANT_KEY`    | `oCLvKupdjTqKc89`                                         |
   | `SLACK_BOT_TOKEN`    | `xoxb-…` token for the existing Nok Slack bot             |

   The Slack bot needs these scopes: `chat:write`, `files:write`. Double-check
   at **api.slack.com → Your apps → OAuth & Permissions**. The bot also needs
   to be invited into `#resale` (`/invite @YourBotName` in that channel) — bots
   can't post to channels they haven't been added to.

2. First run: go to **Actions → Nok Daily Orders → Run workflow** to test
   manually. If that succeeds and you see the post in `#resale`, the cron is
   set up and future runs happen automatically.

## Timing

The workflow cron is `10 11 * * *` = 11:10 UTC daily.

- During **EDT** (mid-Mar to early Nov): 7:10 AM ET
- During **EST** (early Nov to mid-Mar): 6:10 AM ET

Summary is about yesterday's numbers, so the ~1hr DST shift is harmless.

## Outputs

- **Slack post in `#resale`** — text summary + `nok_orders_YYYY-MM-DD.xlsx` attachment.
- **GitHub Actions artifact** — `nok-orders-dump` on every run, retained 30 days. Useful for debugging.

If the run fails at any step (pull, brand mapping guardrail, xlsx build,
Slack post), it posts a `*Daily orders refresh — FAILED*` message in `#resale`
with the reason and exits 1 so the Actions run is marked failed too.

## Adding a new brand / fixing an unmapped SKU

When a new marketplace adds a brand you haven't seen, the audit will flag the
unmapped SKU, the run will fail, and the Slack FAILURE message will list the
SKUs with their sample descriptions and storefronts. Add rules to
`references/brand_map.json`:

- **Exact one-off SKUs** → `sku_overrides`
- **SKUs that share a description pattern** → `description_rules` (preferred)
- **SKUs without useful descriptions (e.g., b2b)** → `sku_rules`

Each rule uses Python regex syntax with optional flags (`"i"` for case-insensitive).
Commit, push, re-run the workflow manually to confirm.

## Adding a new marketplace

If a new `order_source` appears (e.g., `TikTokShop_US`), the Slack message
will render it with its raw name. Edit the `MARKETPLACE` dict at the top of
`scripts/run_daily.py` to add the friendly display name and commit.

## Relationship to the extensiv-im Cowork skill

`scripts/client.py`, `scripts/brand_mapper.py`, and `references/brand_map.json`
are copied from Nick's `extensiv-im` Cowork skill. If the skill's code drifts
(e.g., new IM endpoints, new brand rules), sync those three files into this
repo. One-way sync is fine — the skill is the source of truth for interactive
agent use, this repo is the source of truth for the cron job.

## Debugging

- **Check a specific run**: Actions tab → click the run → expand each step.
- **Inspect the xlsx**: download the `nok-orders-dump` artifact.
- **Re-run manually**: Actions → Nok Daily Orders → Run workflow button.
- **Disable temporarily**: comment out the `schedule:` block or disable the workflow.
- **Locally**: `pip install -r requirements.txt` then
  `IM_MERCHANT_USER=... IM_MERCHANT_KEY=... SLACK_BOT_TOKEN=... python scripts/run_daily.py`

## Deprecating the old Cowork task

Once this has run cleanly for ~3 days in a row, disable the `nok-daily-orders-slack`
Cowork scheduled task (via the scheduled-tasks UI or `update_scheduled_task` with
`enabled: false`) so you don't get duplicate Slack posts.
