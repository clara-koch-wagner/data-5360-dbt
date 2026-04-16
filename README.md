# Eco Essentials Marketing Data Mart

**USU Data Warehousing — Final Project**
**Team:** Clara Koch & Abigail Ford

---

## Project Overview

This project delivers a full end-to-end data warehousing solution for Eco Essentials, a consumer goods brand. The goal was to build a live analytics dashboard that enables leadership to monitor sales trends and evaluate the performance of marketing email campaigns — identifying which campaigns drive the most conversions and revenue.

The project spans four deliverables, each representing a distinct layer of the modern data stack:

| Deliverable | Focus | Tools |
|---|---|---|
| D1 | Data Ingestion | Fivetran, Snowflake |
| D2 | Data Warehouse Design | Snowflake, Star Schema |
| D3 | Data Transformation | dbt (data build tool) |
| D4 | Dashboard & Visualization | Node.js, Express, Chart.js |

---

## Architecture

```
Source Data
    │
    ▼
Fivetran (ELT ingestion)
    │
    ▼
Snowflake (Cloud Data Warehouse)
    │
    ▼
dbt (Transformation & Modeling → Star Schema)
    │
    ▼
Node.js REST API (Live query layer)
    │
    ▼
Chart.js Dashboard (Browser-based visualization)
```

---

## Deliverable 1 — Data Ingestion with Fivetran

We used **Fivetran** to connect source systems and load raw sales and email campaign data into Snowflake. Fivetran handled schema inference and incremental syncing automatically, eliminating the need for manual ETL scripts.

**Key takeaway:** Managed connectors dramatically reduce the overhead of keeping warehouse data fresh. Fivetran's automated syncing means data pipelines don't need to be maintained by hand.

---

## Deliverable 2 — Snowflake Data Warehouse

We designed and built a **star schema** in Snowflake, organizing data into fact and dimension tables optimized for analytical queries.

**Fact Tables:**
- `ECO_FACT_SALES` — transactional sales records
- `ECO_FACT_EMAIL` — email event records (sent, open, click, convert)

**Dimension Tables:**
- `ECO_DIM_CUSTOMER`, `ECO_DIM_PRODUCT`, `ECO_DIM_CAMPAIGN`, `ECO_DIM_DATE`, and more

**Key takeaway:** A well-designed star schema makes complex analytical queries simple and fast. Separating facts from dimensions is foundational to any scalable data warehouse.

---

## Deliverable 3 — Transformation with dbt

We used **dbt (data build tool)** to write modular SQL transformations that clean, join, and aggregate raw Fivetran data into the star schema models above. dbt's lineage tracking and documentation features made it easy to understand how data flows through the pipeline.

**Key takeaway:** dbt brings software engineering best practices (version control, testing, modularity) to SQL transformations. It's become an industry standard for the transformation layer of the modern data stack.

---

## Deliverable 4 — Live Analytics Dashboard

We built a **Node.js + Express** web application that connects directly to Snowflake via the Snowflake SQL REST API using JWT key-pair authentication. The frontend uses **Chart.js** to render all visualizations.

### Dashboard Features

- **KPI Cards** — Total revenue, total orders, average order value, campaigns tracked
- **Email KPIs** — Emails sent, open rate, click rate, conversion rate
- **Sales by Month** — Monthly revenue trend line
- **Campaign Performance** — Revenue attributed per campaign (bar chart)
- **Email Funnel by Campaign** — Sent → Opened → Clicked → Converted per campaign
- **Email Conversion Rate Over Time** — Trend of campaign effectiveness
- **Top Products by Revenue** — Best-selling products

### Tech Stack

- **Backend:** Node.js, Express 5
- **Authentication:** RSA key-pair JWT (`jsonwebtoken`, `crypto`)
- **Data warehouse:** Snowflake (SQL REST API `/api/v2/statements`)
- **Frontend:** Vanilla JS, Chart.js 4.4.1
- **Fonts:** DM Serif Display, DM Sans (Google Fonts)
- **Config:** dotenv

### Key Implementation Details

Snowflake authentication uses **key-pair JWT** rather than username/password. The server generates a signed JWT on each request using an RSA private key, which Snowflake verifies against the public key registered to the user account. This is the recommended approach for server-to-server Snowflake connections.

All chart data is fetched from REST API endpoints (`/api/sales-by-month`, `/api/campaign-performance`, etc.) that execute parameterized SQL against the Snowflake warehouse and return JSON.

---

## Key Takeaways

- **The modern data stack is modular.** Each layer (ingestion → storage → transformation → visualization) can be swapped independently. We used Fivetran + Snowflake + dbt + Node.js, but the concepts apply across tools.
- **Star schemas are still the gold standard** for analytical workloads. The simplicity of joining one fact table to a few dimensions makes queries intuitive and fast.
- **Authentication matters.** Connecting a live application to a data warehouse requires secure, token-based auth — not hardcoded credentials.
- **Real-time dashboards close the gap** between data and decisions. Leaders can check campaign performance without waiting on a weekly report.


*Built for USU Data Warehousing — Spring 2026*
