# Industry Taxonomy Plan

Date: 2026-03-29

## Goal

Make industry filtering feel like something a real job seeker would actually use, not a form taxonomy.

Two design decisions:

- Use the same enum list for both `industry_primary` and `industry_tags`.
- Allow jobs from the same company to vary in tags when that is genuinely true for the role.

## Problems With The Current Taxonomy

The current `Industry` enum in [parse.py](/Users/aburkard/fun/dope-jobs-pipeline/parse.py) is workable as a `v0`, but it has three structural problems:

1. It overuses broad buckets.
   - `saas_software` currently absorbs a huge share of the corpus, including companies that users would likely want to distinguish:
     - Vercel
     - Datadog
     - Cloudflare
     - MongoDB
     - Stripe
     - Databricks
     - GitLab
     - Reddit
     - Airbnb
     - Spotify

2. It mixes different kinds of concepts.
   - Some values describe business domain:
     - `financial_services`
     - `healthcare`
     - `education`
   - Some describe product/media type:
     - `gaming`
     - `entertainment_media`
   - Some describe technical modality:
     - `ai_ml`
     - `robotics`
     - `semiconductors`

3. It is missing several buckets users are likely to want.
   - `developer_tools_infra`
   - `consumer_social`
   - `investing_trading`
   - `staffing_recruiting_bpo`
   - split `space_aerospace` from `defense_public_safety`

## Evidence From The Live Parsed Corpus

Current active parsed job counts by industry:

- `saas_software`: `1325`
- `financial_services`: `729`
- `ai_ml`: `593`
- `gaming`: `422`
- `aerospace_defense`: `391`
- `entertainment_media`: `389`
- `marketing_advertising`: `379`
- `hospitality_tourism`: `251`
- `logistics_supply_chain`: `205`
- `retail_ecommerce`: `161`

This is the clearest sign that `saas_software` is too broad.

Examples of current drift across industries for the same company:

- `Reddit`
  - `ai_ml`
  - `entertainment_media`
  - `marketing_advertising`
  - `saas_software`

- `Stripe`
  - `cryptocurrency_web3`
  - `financial_services`
  - `saas_software`

- `Cloudflare`
  - `cybersecurity`
  - `saas_software`

- `Aeva`
  - `aerospace_defense`
  - `automotive`
  - `manufacturing`
  - `robotics`
  - `semiconductors`
  - `other`

- `Airbnb`
  - `gaming`
  - `hospitality_tourism`
  - `retail_ecommerce`
  - `saas_software`

Some of that drift is legitimate. Some of it is a symptom of the taxonomy not having the right conceptual buckets.

## Proposed Enum List

This is the proposed combined enum list for both `industry_primary` and `industry_tags`.

```python
[
  "ai_ml",
  "developer_tools_infra",
  "enterprise_software",
  "cybersecurity_identity",
  "fintech_payments_banking",
  "investing_trading",
  "insurance",
  "crypto_web3",
  "healthcare_services",
  "biotech_pharma_life_sciences",
  "education_edtech",
  "consumer_social",
  "media_entertainment",
  "gaming",
  "advertising_marketing",
  "commerce_marketplaces",
  "consumer_goods_brands",
  "food_beverage",
  "travel_hospitality",
  "climate_energy_utilities",
  "transportation_logistics",
  "manufacturing_industrials",
  "robotics_autonomy",
  "semiconductors_hardware",
  "space_aerospace",
  "defense_public_safety",
  "government_public_sector",
  "real_estate_construction",
  "telecommunications_networking",
  "agriculture",
  "legal",
  "consulting_professional_services",
  "nonprofit_philanthropy",
  "staffing_recruiting_bpo",
  "other",
]
```

## Why These Buckets

### Core tech/product buckets

- `ai_ml`
- `developer_tools_infra`
- `enterprise_software`
- `cybersecurity_identity`

These separate "AI", "devtools/cloud/data infra", general business software, and security/identity in a way users actually care about.

### Financial buckets

- `fintech_payments_banking`
- `investing_trading`
- `insurance`
- `crypto_web3`

`financial_services` is too broad. Users often mean very different things by Stripe/Chime, Robinhood/Point72, insurance, and crypto.

### Healthcare and science

- `healthcare_services`
- `biotech_pharma_life_sciences`

This separates care delivery/platforms from research/biotech/pharma.

### Consumer/media buckets

- `consumer_social`
- `media_entertainment`
- `gaming`
- `advertising_marketing`
- `commerce_marketplaces`
- `consumer_goods_brands`
- `food_beverage`
- `travel_hospitality`

This is more useful than collapsing everything into `entertainment_media` or `retail_ecommerce`.

### Industrial/physical world buckets

- `climate_energy_utilities`
- `transportation_logistics`
- `manufacturing_industrials`
- `robotics_autonomy`
- `semiconductors_hardware`
- `space_aerospace`
- `defense_public_safety`
- `real_estate_construction`
- `telecommunications_networking`
- `agriculture`

This gives enough precision for robotics, space, logistics, climate, and hard-tech companies without turning the list into a full NAICS hierarchy.

### Institutional/service buckets

- `government_public_sector`
- `legal`
- `consulting_professional_services`
- `nonprofit_philanthropy`
- `staffing_recruiting_bpo`

These matter for many non-tech roles and for a general jobs site.

## Example Mappings

These are illustrative, not hardcoded.

- `Vercel`
  - primary: `developer_tools_infra`
  - tags: `ai_ml`

- `Datadog`
  - primary: `developer_tools_infra`
  - tags: `ai_ml`

- `Cloudflare`
  - primary: `cybersecurity_identity`
  - tags: `developer_tools_infra`

- `Stripe`
  - primary: `fintech_payments_banking`
  - tags: `enterprise_software`

- `Robinhood`
  - primary: `investing_trading`
  - tags: `fintech_payments_banking`

- `Airbnb`
  - primary: `travel_hospitality`
  - tags: `commerce_marketplaces`

- `Reddit`
  - primary: `consumer_social`
  - tags: `advertising_marketing`, `ai_ml`

- `Spotify`
  - primary: `media_entertainment`
  - tags: `consumer_social`

- `Databricks`
  - primary: `developer_tools_infra`
  - tags: `ai_ml`

- `Anthropic`
  - primary: `ai_ml`
  - tags: `developer_tools_infra`

- `Anduril`
  - primary: `defense_public_safety`
  - tags: `ai_ml`, `robotics_autonomy`

- `Relativity Space`
  - primary: `space_aerospace`
  - tags: `manufacturing_industrials`

- `Aeva`
  - primary: `robotics_autonomy`
  - tags: `semiconductors_hardware`, `automotive`

Note:
- `automotive` is intentionally not a top-level bucket in the proposed enum.
- For now, automotive-style companies should generally land in one of:
  - `transportation_logistics`
  - `robotics_autonomy`
  - `manufacturing_industrials`
  - `semiconductors_hardware`

If automotive keeps surfacing as a distinct user need, it can be added later.

## Proposed Data Model

Eventually the schema should move from:

- `industry`

to:

- `industry_primary`
- `industry_tags`

Where:

- `industry_primary` is exactly one value from the enum
- `industry_tags` is zero or more values from the same enum list

Recommended product behavior:

- use `industry_primary` for compact display
- use `industry_tags` for filtering and recall

This is better than forcing one field to do both jobs.

## Migration Notes

When implementing this:

1. Replace the current `Industry` enum with the new list.
2. Add `industry_primary`.
3. Add `industry_tags`.
4. Keep the UI filter logic flexible so it can later choose:
   - filter by primary only
   - or filter by any tag match

## Open Questions

These are the only meaningful unresolved questions right now:

1. Should `consumer_social` and `media_entertainment` remain separate?
   - Current answer: yes.

2. Should `enterprise_software` be split further, for example to carve out productivity/collaboration?
   - Current answer: not yet.

3. Does `automotive` deserve to come back as a top-level value?
   - Current answer: probably not yet.
