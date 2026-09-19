# Verified cost inputs (pulled 2026-09-19)

Source: AWS Price List Bulk API, `us-east-1`, plus Anthropic published API pricing.
Use these numbers for all arithmetic. Do not invent rates. Regional premiums for
`eu-central-1` / `ca-central-1` run roughly 5-15% above `us-east-1`; say so if it matters.

| Item | Rate |
|---|---|
| SQS standard requests | $0.40 / million (tiers down to $0.30, then $0.24 at volume) |
| SQS FIFO requests | $0.50 / million (tier 1) |
| Lambda requests | $0.20 / million |
| Lambda compute (ARM) | $0.0000133334 / GB-second |
| Lambda compute (x86) | $0.0000166667 / GB-second |
| DynamoDB on-demand writes | $0.625 / million write request units |
| DynamoDB on-demand reads | $0.125 / million read request units |
| DynamoDB storage | $0.25 / GB-month |
| DynamoDB Streams reads | $0.20 / million read request units |
| S3 Standard storage | $0.023 / GB-month |
| Claude Sonnet 5 | $2.00 / $10.00 per million input / output tokens |
| Claude Haiku 4.5 | $1.00 / $5.00 per million input / output tokens |
| Claude Opus 5 | $5.00 / $25.00 per million input / output tokens |
| Prompt caching | cache reads ~90% below base input rate |
| Batch API | 50% off both input and output |

Note: DynamoDB on-demand is now $0.625/$0.125 per million. The widely quoted
$1.25/$0.25 figures are stale; do not use them.

## Problem constants from the assignment

- 4,000 firms; 800,000 active engagement files; 40 products.
- Engagements per firm are skewed: median ~40, largest ~40,000.
- Template updates publish roughly once per week per product (~40 publishes/week,
  ~173/month).
- Loading one engagement to read its state takes ~1 minute. Hard constraint.
- Data residency: EU and Canada for some firms.
- All-in monthly run cost must stay under a budget the assignment leaves as «$X».
