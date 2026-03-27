# PURPOSE
Remove weak, generated, stale, or duplicate repo content without touching live runtime state.

# WHEN TO USE IT
Use when markdown, caches, wrappers, logs, or duplicate assets are cluttering control or onboarding paths.

# WHAT IT MAY CHANGE
Docs, generated artifacts, caches, logs, and duplicate control content.

# WHAT IT MUST NOT CHANGE
Runtime Python, configs in use, database files, secrets, or real workflows.

# EXPECTED SAFE OUTPUT
Printed path list, deleted path list, rejected uncertain files, and zero runtime drift.

# FAILURE BEHAVIOR
Move uncertain cleanup candidates to rejected status and stop before deletion.
