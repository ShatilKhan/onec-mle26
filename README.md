# MLE Take-Home: Company Recommendation System

## Solution

Consumer + similarity service built on the scaffold below. Python 3.14 via `uv`, Docker Engine + Compose,
`just`. Pinned deps in `uv.lock`. Only Postgres and RabbitMQ run in Docker; consumer and service are host
processes. Full decisions and diagrams: [`DESIGN.md`](DESIGN.md).

### Architecture

```mermaid
flowchart LR
  F["data/feed.jsonl"] --> P["producer/publish.py"]
  P -->|"company-events (durable)"| Q[("RabbitMQ")]
  Q --> C["consumer"]
  C -->|"poison"| DLQ[("company-events.dlq")]
  C -->|"latest-wins upsert + embedding"| DB[("Postgres + pgvector")]
  C -->|"processed_messages"| DB
  S["FastAPI service"] -->|"HNSW top-40 + soft bonus"| DB
  W["portal / curl"] -->|"similar?limit&offset"| S
  S -->|"cards"| W
  DLQ -.->|"depth check"| J["just dlq-check"]
```

### Setup

Prerequisites: Docker Engine, `uv`, `just`. All managed by `mise` on this machine.

```bash
just setup      # uv sync --locked
just up         # Postgres + RabbitMQ, wait until healthy
```

#### Configuration

All settings come from environment variables; the defaults below work with the Docker Compose services.
Nothing needs changing for a local run.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/companies` | pgvector connection |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | AMQP connection |
| `QUEUE` | `company-events` | input queue (owned by the registry producer) |
| `DLX` | `company-events.dlx` | direct exchange we declare for poison |
| `DLQ` | `company-events.dlq` | poison queue; depth checked by `just dlq-check` |
| `EMBED_MODEL` | `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` | 384-d CPU model, fetched by `fastembed` on first run |
| `LIMIT_MAX` | `20` | maximum page size |
| `CANDIDATE_WINDOW` | `40` | candidates fetched before paging / soft bonus |
| `DLQ_BASELINE` | `25` | expected poison depth after one full replay |
| `INDUSTRY_BONUS` | `0.02` | ranking bonus for same industry |
| `STATE_BONUS` | `0.01` | ranking bonus for same state |
| `LOG_LEVEL` | `INFO` | consumer log level |

If host ports were overridden for Compose, set `DATABASE_URL` and `RABBITMQ_URL` to match. Changing
`EMBED_MODEL` to a model with different dimensionality requires a `vector(...)` column migration and
re-embedding — see [`DESIGN.md`](DESIGN.md).

### Running

```bash
just publish    # replay 2,518 feed messages
just consumer   # drain queue into Postgres (Ctrl+C to stop)
just service    # API on http://127.0.0.1:8000 (second terminal)
```

Endpoints: `GET /health`, `GET /companies/{registrationNumber}/similar?limit=4&offset=0`.

### Manual verification

```bash
curl http://127.0.0.1:8000/health
curl 'http://127.0.0.1:8000/companies/SYN-001550/similar?limit=4&offset=0'
curl 'http://127.0.0.1:8000/companies/SYN-001550/similar?limit=4&offset=4'
docker compose exec -T postgres psql -U postgres -d companies -c \
  "SELECT count(*) total, count(*) FILTER (WHERE active) active,
   count(*) FILTER (WHERE embedding IS NOT NULL) embedded,
   count(*) FILTER (WHERE embedding_surrogate) surrogate,
   count(*) FILTER (WHERE NOT active) struck FROM companies;"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages
just publish && just consumer   # replay: counts must not change
```

Expected after one replay: `company-events` 0, `company-events.dlq` 25, `companies` 2,022 rows
(2,014 active and embedded, 39 surrogate, 8 struck-off), `processed_messages` 2,450. A second replay adds
nothing. `SYN-001550` returns `SYN-000539` first — matching the portal mock.

### Automated verification

```bash
just test        # parser / normalisation unit tests
just dlq-check   # depth vs baseline; exit 1 on spike
```

`just dlq-check` is a one-shot script (stdlib only) suitable for cron or CI. A scheduled GitHub Actions run
is left as future scope because a GitHub-hosted runner cannot reach local Docker services.

### Findings and results

Verified locally on Docker Engine 29.9.3, Postgres 16 + pgvector 0.8.7, RabbitMQ 3.13.7, Python 3.14.10:

- Feed is dirty by design: 5 malformed JSON, 10 unusable registration numbers, 6 `null` lines, 9 non-dict
  `data` values; 25 distinct poison IDs → DLQ baseline 25.
- Feed is out of order (`SYN-000155` has a 22:03 event before 15:39). Latest-wins by normalized source
  `updatedAt` prevents regressions; replay converges because ties break by `message_id`.
- `updatedAt` has four shapes: ISO with `+08:00`, ISO `Z`, naive ISO (assumed Malaysia `+08:00`), and int
  epoch-ms. Missing values use a 1970 sentinel.
- `fastembed` does not support `intfloat/multilingual-e5-small`; switched to fastembed-supported
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2` (384-d, multilingual). No chunking needed:
  p99 description length is 9 words.
- Hard industry/state filtering was rejected: median industry+state cell size is 3, and the portal mock is
  cross-state/cross-industry. A small same-industry/state bonus is used over a top-40 candidate window.
- Local run: API returned sensible neighbours, page 2 returned different rows, 404 for unknown registration,
  422 for `limit + offset > 40`. `just test`: 5 passed. `just dlq-check`: depth 25 OK.
- Local counts after one clean replay: 2,518 messages published → 2,022 companies, 2,014 active/embedded,
  39 surrogate vectors, 8 struck-off, 2,450 processed messages, DLQ depth 25, input queue 0.
- Query for `SYN-001550` returned `SYN-000539` first, then `SYN-000263` and `SYN-001650` — all cafés /
  coffee shops, cross-state as in the portal mock. Reported score clamped to `1.0`.
- Replaying the feed a second time added 0 rows, 0 processed messages, 0 DLQ messages — idempotency verified.
- Learning references and the session transcript: [`sessions/`](sessions/sessions.md).

---

Welcome to the **Machine Learning Engineer** take-home from the OneCredit Data & AI team. It's a simplified version of
a system we run in production, where it powers company recommendations and lead generation.

**Who it's for:** people early in their career, with 0 to 2 years of experience. Fresh graduates are welcome.

If you applied, thanks for your time. If you didn't, we hope this helps you learn something new, such as RabbitMQ or
pgvector.

Assessment version: **2026.11** (`mle-v2026.11`). What changed between versions is in [`CHANGELOG.md`](CHANGELOG.md).

## The scenario

When someone opens a company's page on the OneCredit portal, we want to show a **"Similar companies"** panel next to
it. Someone looking at a coffee shop chain in Selangor should see other companies that do similar things.

The registry team already publishes every company change to a RabbitMQ queue. Your job is to build the service that
powers that panel from the feed.

### What the user sees

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  OneCredit Portal                                         Search companies…  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Sinar Teguh Bakery Sdn. Bhd.                                   SYN-001550   │
│  Food & Beverage · Selangor                                                  │
│  Operating cafes and coffee shops in the Klang Valley                        │
│                                                                              │
│  Similar companies                                                           │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Seri Sejahtera Food Sdn. Bhd.      │  │ Everbright Pacific Food        │  │
│  │ Food & Beverage · Johor            │  │ Sdn. Bhd.                      │  │
│  │                                    │  │ Food & Beverage · Kuala Lumpur │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Gemilang Delima Food Sdn. Bhd.     │  │ Pacific Everbright Logistik    │  │
│  │ Food & Beverage · Johor            │  │ PLT                            │  │
│  │                                    │  │ Logistics · Kuala Lumpur       │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

Clicking a card opens that company's page, with its own "Similar companies" panel.

### How it works for the user

```mermaid
sequenceDiagram
    actor User as Portal user
    participant Portal as OneCredit portal
    participant Service as Your service
    participant DB as Postgres + pgvector
    User->>Portal: Opens the page for SYN-001550
    Portal->>Service: Similar companies for SYN-001550
    Service->>DB: Nearest neighbours of SYN-001550
    DB-->>Service: Closest companies
    Service-->>Portal: Similar companies, most similar first
    Portal-->>User: Page with the "Similar companies" panel
    User->>Portal: Clicks Seri Sejahtera Food Sdn. Bhd.
    Portal->>Service: Similar companies for SYN-000539
```

### How the data gets there

```mermaid
flowchart LR
    P[Registry feed<br/>producer] -->|company-events| Q[(RabbitMQ)]
    Q --> C[Your consumer]
    C -->|embed descriptions| E[Embedding model]
    C --> D[(Postgres + pgvector<br/>similarity search)]
    A[Your service] --> D
    W[OneCredit portal] --> A
```

### Notes from the registry team

> The feed merges a few upstream sources and we don't validate what they send. Delivery is at-least-once, and every
> message has an AMQP `message_id`. Each event carries the time the change happened at the source in `updatedAt`. We own
> the `company-events` queue and declare it as durable with no extra arguments. We can't change the format before your
> launch, so please work with what's there.

### What the portal needs

- Recommendations that reflect the latest information we've received about each company.
- No lost companies. If a company is in the feed, users expect to see it on the portal and in other companies'
  recommendations.
- Fast enough to render while the company page loads.

## The task

1. Build a **consumer** that reads the `company-events` queue and stores each company in Postgres, with an embedding
   of its business description in pgvector.
2. Build the **service** the portal calls: given a company, return its most similar companies, with what the portal
   needs to draw each card in the mock above. Run the similarity search in Postgres with pgvector. Matching companies
   on their nature of business is enough for a first version.
3. Write a short `DESIGN.md` covering the decisions you made, the assumptions you made where the scenario was unclear,
   what you would change to handle 10x the volume, and what's left.
4. Optional, up to one hour of your six: go deeper in one area you're strong in. For example: how good the
   recommendations are, latency at a realistic scale, how you'd monitor this in production, or a design for explaining
   a recommendation with an LLM (a design only; no paid APIs). Write it up in `DESIGN.md`.

The rest is your call: the schema, the embedding model, your tests and tooling, and the API, including what the portal
gets back when something goes wrong. Tell us in your own README how to start your consumer and service, and we'll
follow it.

Where the scenario leaves something open, make a reasonable call and write it down. There isn't one right answer.

## Embeddings

Use an open-source embedding model that runs on a laptop CPU. Don't call a paid API. Some options:

- `BAAI/bge-small-en-v1.5`, a small CPU model (384 dimensions), through
  [fastembed](https://qdrant.github.io/fastembed/) or sentence-transformers. The panel in the mock above comes from it.
- A placeholder while you build, such as a hashed bag-of-words vector, as long as you say so in `DESIGN.md`.

## Getting started

You need Docker, [uv](https://docs.astral.sh/uv/) (it installs Python 3.14 for you) and
[just](https://github.com/casey/just). Setup takes about 5 minutes and doesn't count towards your time.

```bash
just setup      # install the producer's dependencies
just up         # start Postgres (with pgvector) and RabbitMQ
just publish    # publish the registry feed to the queue
just down       # stop everything and delete the data
```

Postgres is at `postgresql://postgres:postgres@localhost:5432/companies`, and the RabbitMQ management UI is at
http://localhost:15672 (user `guest`, password `guest`). If those ports are taken on your machine, export
`POSTGRES_PORT`, `RABBITMQ_PORT` or `RABBITMQ_UI_PORT` in your shell before `just up`; `just publish` uses the same
`RABBITMQ_PORT`.

`just publish` sends the whole feed every time you run it. To start again from an empty queue and database, run
`just down` and then `just up`.

## What's already here

```
.
├── data/feed.jsonl        # the registry feed: 2,518 messages about 2,000 synthetic Malaysian companies, duplicates included
├── producer/publish.py    # replays the feed onto the queue
├── docker-compose.yml     # Postgres with pgvector, and RabbitMQ
└── Justfile               # setup, up, publish, down
```

Add whatever else you need (`uv add` for libraries).

## How to submit

1. Click **Use this template** → **Create a new repository**, and make it **public**. Any name is fine.
2. Build your solution in your copy, with `DESIGN.md` at the top level.
3. When you're done, fill in the [submission form](https://docs.google.com/forms/d/e/1FAIpQLSdjCsXP5jFrKrIi4krLVS8Sq0rhM4mLlhycpsAo34sSj4h3FA/viewform) with a link to your repo and your résumé.

If the setup itself is broken (Docker, the services or the data won't start), open a
[Setup problem](https://github.com/OneCreditMY/mle-take-home/issues/new?template=setup-problem.yml) issue. Issues are public, so don't put your
solution or personal details in one.

## Time

Plan for **4 to 6 hours**, within 7 days of creating your copy, and stop at 6 hours. If you run out of time,
write down what you'd do next. A clear, honest "here's what's left" counts in your favour.

## What we look at

- Does it work, and does it keep working?
- How you tested it.
- How you reasoned about your choices and assumptions in `DESIGN.md`.
- How readable your code is.

We don't expect a perfect or complete solution.

## Using AI tools

Go ahead. Tell us in `DESIGN.md` which tools you used and for what. In the interview we'll ask you
to walk us through your code, so make sure you understand everything you submit.

## After you submit

- We review your work as soon as we can.
- If we move forward, the next interview is a walkthrough of your code with us.
- Either way, you'll get written feedback by email.
- Your repo stays yours. Keep it public as a portfolio piece if you like.

## About the data

Everything in this repo is synthetic. We generated it for this assessment.

- The companies, names, registration numbers, descriptions and events are made up. Any resemblance to a real company
  or person is a coincidence.
- No OneCredit customer data, credit data or production data is in this repo, and none was used to create it.
- Registration numbers use a made-up `SYN-` format, so they can't be mistaken for real Companies Commission of
  Malaysia (SSM) numbers.
- Your copy is public, so keep secrets, credentials and personal data out of it.
