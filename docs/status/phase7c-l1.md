> Status: CURRENT (2026-10-29) the ledger for phase 7c step L1 (Kronuz/task); nothing supersedes it.

# phase7c-l1

- Design: Xapiand `docs/design/phase7c.md` (D2, D3, D5, D14, D16, section 5 L1) at `Xapiand:f3bd508f8`;
  reviews: `docs/reviews/phase7c-design.md` (R2 closed by this step, R8 blocks landing, C7 untouched).
- Branch / worktree: `phase7c/l1-task` at `~/code/Kronuz/task-wt/phase7c-l1`; head: see `git log`.
- State: implementing
- Done criteria (from the lead's brief):
  - [ ] `event`, `async_mutex` with `lock_until`, `await_asio`, per-resume slice, `demoted()`, destructor asserts
  - [ ] tests for every primitive
  - [ ] a mutant per claim, killed
  - [ ] ASan/UBSan and TSan builds clean (Homebrew LLVM)
  - [ ] R8 probe orders A and B run ASan-clean against this code
  - [ ] `cpp-libraries/tools/preflight.sh` before push

## Log
- 2026-10-29: started.
