# ATS Gaps

## Workable

- `domain` is not currently extracted.
  The public Workable widget API and hosted board HTML do not expose a trustworthy company website/domain for boards like `telegraph`.
- `logo_url` is not currently extracted.
  The visible company logo appears to be client-rendered and is not present in the stable public JSON endpoints we use.

For now, Workable domains/logos should be treated as a separate enrichment problem, not part of the scrape path.
