# PURPOSE
Diagnose and patch one verified issue with the smallest safe change.

# WHEN TO USE IT
Use when runtime behavior is wrong, unstable, or out of contract.

# WHAT IT MAY CHANGE
Only the minimum files required to fix the verified issue.

# WHAT IT MUST NOT CHANGE
Architecture, secrets, schema, unrelated files, or stable runtime paths without proof.

# EXPECTED SAFE OUTPUT
Short list of changed files, validation result, and remaining risk if any.

# FAILURE BEHAVIOR
Stop after evidence collection and return a blocked status instead of guessing.
