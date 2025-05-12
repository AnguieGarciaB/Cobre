# BI

Prompts:
*Context & Business Problem:*

```python
Cobre, a fintech company, needs a data-driven approach to evaluate marketing and sales performance. The goal is to understand which channels lead to the highest-quality and fastest-converting leads, forecast future leads, and optimize marketing efforts based on ROI.

Task Overview:
Design a conceptual data model that unifies leads, marketing sources, enrichment metadata, and conversion outcomes.


Data Models:
Leads Qualified (MQL) Data:
mql_id: Unique ID for the marketing-qualified lead.
first_contact_date: Date of first contact.
landing_page_id: ID of the landing page where the lead signed up.
origin: Source of the lead (e.g., organic, paid).

Leads Closed (Converted Leads) Data:
mql_id: ID of the marketing-qualified lead.
seller_id: ID of the associated seller.
sdr_id: Sales Development Rep ID.
sr_id: Sales Rep ID.
won_date: Date of conversion to a paying customer.
business_segment: Business segment of the lead.
lead_type: Type of lead (Hot, Warm, Cold).
has_company: Whether the lead has a registered company (Yes/No).
has_gtin: Whether the lead has a Global Trade Item Number (Yes/No)."
```

```python
what could be the best arquitecture of data in dbts snowflake to meet the goal
```