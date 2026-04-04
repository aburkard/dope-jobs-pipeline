Production search notes:

- This repo is the source of truth for the live Meilisearch `jobs` index.
- The `jobs` index primary key must be `meili_id`, not raw `id`. Raw job ids can contain `.` and are not safe as Meili document ids.
- Keep both fields in documents: `id` is the canonical job id; `meili_id` is the deterministic Meili-safe surrogate from `public_ids.meili_safe_job_id`.
- Do not run a full Meili `jobs` reload unless it is truly necessary (for example index corruption, primary-key migration, or an intentional full rebuild). Prefer targeted reloads or partial document updates so we do not reinsert and re-embed the whole corpus.
- With the embedder enabled, partial Meili updates must still include enough searchable text (for example `title`/`company`) for embedding to succeed; a tiny field-only patch can fail with `vector_embedding_error`.
- If the index gets mixed/corrupted, the safe repair is: delete `jobs`, then run a clean DB-backed full reload.
- Internal background Meili traffic should use `https://search-internal.dopejobs.xyz`, not `search.dopejobs.xyz`.
- `search-internal.dopejobs.xyz` is Cloudflare-proxied, protected by Cloudflare Access service-token headers, and origin-blocked for non-Cloudflare source IPs.
- The Hetzner origin exempts Cloudflare IP ranges from the coarse `80/443` connection throttle. If internal access breaks, first verify Cloudflare Access headers and then refresh the Cloudflare IP allowlist on the origin.
