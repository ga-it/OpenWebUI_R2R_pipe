# Changelog

## 2.2.1 • 2026-11-27

- Validate the original OpenWebUI question before passing it to Nextcloud R2R.
  Keep the existing 3–12,000-character bound on the search question.
- Forward authorized OpenWebUI source excerpts and native media to the existing
  answering model without adding them to the search query. This applies to all
  attachment formats that the OpenWebUI ingestion and model routes support.
