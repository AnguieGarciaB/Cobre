# BI

Prompts:
*Context & Business Problem:*

```python
"""
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
"""
```

```python
"what could be the best arquitecture of data in dbts snowflake to meet the goal"
```

*Funnel & Attribution Analytics*

```python
"I am preparing to create a visualization that answers the following question:
'Which acquisition channels (e.g., paid, organic, email, referral) generate the highest-quality and fastest-converting leads?'

To address this, I will evaluate and compare channels using the following metrics:
Conversion speed (average days to conversion or win)
Win rate (from total leads to deals won)
Lead quality progression (MQL to SQL to Won conversion rates)

The data includes multiple dimensions:
Lead Source and Lead Type vs. Average Days to Conversion
Lead Source vs. Total Leads, Won Leads, Win Rate, and Avg Days to Win
Lead Source vs. MQL, SQL, Won, and Funnel Conversion Rates

I aim to use these to visualize and identify the most effective and efficient lead sources."
```

```python
"I have three DataFrames in Python, which are named as follows:
df_avg_conv – contains data related to average conversions.
df_first_att – contains data on the first attempts made in the acquisition process.
df_conversion_rates – contains data on conversion rates across various acquisition channels.
I would like to perform data visualization to answer the question: Which acquisition channels bring in the highest-quality and fastest-converting leads?
Could you please help me with Python code to generate the necessary visualizations to address this question?"
```