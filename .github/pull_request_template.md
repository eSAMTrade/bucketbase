### Title
`<type>`: `<short summary>`

### Ticket / Task
- [Trello|Jira] link: `<URL>`

### What & Why
"Builds X so that Y can Z" – two sentences max.

### Scope
- [ ] New feature
- [ ] Bug fix
- [ ] Refactor / tech-debt
- [ ] Test / tooling only

### Checklist (self-review)
See Section 5.

### Risk / Impact
- Latency critical path?  ☐ Yes  ☐ No
- External API contract change? ☐ Yes  ☐ No
- Migration steps required?  ☐ Yes  ☐ No

### Screenshots / Logs / Benchmarks
(only if relevant)

### Author Self-Review Checklist
- [ ] **Single Ticket** — PR addresses only one business requirement / Trello card.
- [ ] **Minimal Diff** — no unrelated refactors, commented-out code, or debug prints.
- [ ] **Compiles & Tests Pass** — `pytest -q` / `mvn test` clean locally.
- [ ] **No Dead Code** — every new unit is called or covered by tests.
- [ ] **Naming & Clarity** — identifiers are self-explanatory; no overloaded meanings.
- [ ] **Docs Updated** — README, wiki, or docstrings updated where behaviour changed.
- [ ] **Performance Tagged** — if touching hot path, attach micro-benchmarks or profiler diff.
- [ ] **Config & Secrets** — no plaintext credentials; configs externalised.
- [ ] **Rollback Ready** — change can be reverted with `git revert` without dependency hell.
- [ ] **Checklist Acknowledged** — I would merge this myself if I were the reviewer.
- [ ] **LLM Review** — PR has been reviewed by the approved LLM tool and suggestions addressed.
    - [ ] **IDE Type Checker** — PyCharm/Pylance recommendations were addressed where applicable.
    - [ ] **Optional mypy** — (Recommended) Ran `python/scripts/run_mypy.bat` locally with no critical errors

### Resources:
- [PR guidelines](https://wiki.esamtrade.com/en/engineering/pr-guidelines)