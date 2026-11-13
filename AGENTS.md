# Agent instructions

## Keep Git branches in sync

- Before editing, check the worktree and branch, fetch the configured GA remote,
  and explicitly refresh the target branch (usually `main`). Fast-forward a
  clean local base. If the checkout contains someone else's edits, preserve them
  and use a separate worktree.
- Checkpoint after each significant change, at least every two hours of active
  work, and before pausing or handing off. A significant change is a completed
  fix, functional slice, migration, or coordinated documentation update. Review
  the diff, run focused checks, commit only the task's files, and push the
  checkpoint to the appropriate GA branch. If work is unfinished, push a coherent
  WIP checkpoint on the topic branch and record the remaining checks.
- At each checkpoint, fetch and compare the topic branch with both its remote
  counterpart and the merge target. Integrate remote work before pushing. Use
  fast-forward updates where possible and merge a diverged shared branch without
  rewriting published history. Resolve conflicts by inspecting both sides and
  preserving the latest intended behavior and distinct changes. Rerun checks
  affected by the integration. On a non-fast-forward rejection, fetch and
  reconcile again. Never force-push to solve divergence.
- Merge completed, verified changes promptly through the repository's normal
  path, including required PR review and CI. After merging, refresh the local
  target and verify its commit matches the remote. Track any open PR, local-only
  commit, or blocked push in the handoff with its branch and next action.
