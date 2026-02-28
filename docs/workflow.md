# GitHub Issues & Project Workflow

## 1. Issue Lifecycle

```
OPEN → IN PROGRESS → IN REVIEW → CLOSED
```

- **OPEN**: Newly created, awaiting assignment or triage.
- **IN PROGRESS**: Someone is actively working on it.
- **IN REVIEW**: Under review (testing, QA, PR review).
- **CLOSED**: Completed and verified. Do not leave resolved issues open.

## 2. Roles & Responsibilities

| Role | Responsibilities |
|------|------------------|
| **PM (Dan)** | - Triage and prioritize issues (P0/P1/P2/P3)<br>- Assign issues to appropriate agents/owners<br>- Monitor progress and remove blockers<br>- **Close issues after resolution**<br>- Add a resolution comment explaining what was done |
| **DevOps** | - Infrastructure, deployments, Docker, CI/CD<br>- Health checks and monitoring |
| **Developer (coder)** | - Backend implementation, bug fixes<br>- Write unit tests |
| **Tester** | - Write and run test cases<br>- E2E tests (Playwright)<br>- Verify fixes before closure |
| **Docwriter** | - Update documentation alongside code changes |

## 3. Priority Definitions

- **P0 (Critical)**: Production-blocking, security, or data-loss issues. Immediate attention required.
- **P1 (High)**: Core functionality defects. Fix within days.
- **P2 (Medium)**: Feature improvements, non-critical bugs. Fix within weeks.
- **P3 (Low)**: Documentation, minor tweaks. Backlog.

## 4. GitHub Issue Management Rules

1. **When a fix is merged**:
   - PM verifies the fix in the running environment.
   - PM closes the GitHub issue.
   - PM adds a comment summarizing:
     - Root cause (if relevant)
     - Changes made
     - Verification steps
     - Any follow-ups (e.g., create new issue for future work)

2. **Do not leave resolved issues OPEN**. Stale issues create noise and hinder reporting.

3. If an issue cannot be reproduced or is out of scope, add a comment explaining why and close it.

4. Reference related PRs using `Closes #123` or `Fixes #123` in PR description to auto-close on merge (preferred). If auto-close fails, PM must manually close with a comment.

## 5. Communication

- Use GitHub issues for technical tracking.
- Use Feishu group for daily syncs and alerts.
- PM sends daily status summaries to Daniel.

## 6. Exceptions

If an issue must remain open after resolution (e.g., tracking future work), update the title with `[RESOLVED]` prefix and add a comment explaining why it remains open.

---

*Last updated: 2026-02-28 by Dan (PM)*
