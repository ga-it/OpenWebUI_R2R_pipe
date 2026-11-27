# Changelog

## 2.2.2 • 2026-11-27

- Load the full extracted content of chat uploads through OpenWebUI's
  authenticated file loader and send it to the answering model. The R2R query
  planner receives the original question only. Native media is forwarded to the
  answering model in its original message format.

## 2.2.1 • 2026-11-27

- Validate the original OpenWebUI question before passing it to Nextcloud R2R.
  Keep the existing 3–12,000-character bound on the search question.
- Forward authorized OpenWebUI source excerpts and native media to the existing
  answering model without adding them to the search query. This applies to all
  attachment formats that the OpenWebUI ingestion and model routes support.
