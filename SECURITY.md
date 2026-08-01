# Security Policy

## Reporting a Vulnerability

Please report suspected security vulnerabilities privately to **tech@polartrace.com**.
Do not open a public GitHub issue for security reports.

We will acknowledge your report within 72 hours and keep you informed of the fix
timeline. Please include a proof of concept and the affected version if possible.

## Scope notes

The agent captures request metadata (headers, query parameters, bodies and console
logs) and sends it to your configured PolarTrace collector. Sensitive body fields
(passwords, tokens, secrets, credit cards, SSNs) are redacted client-side before
transmission. If you find a way to bypass that redaction, that is in scope and we
want to hear about it.
