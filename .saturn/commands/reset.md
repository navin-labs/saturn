# PURPOSE
Return Saturn to a known-safe local operational baseline without redesigning the system.

# WHEN TO USE IT
Use after drift, failed experiments, or control-layer confusion that must be cleared safely.

# WHAT IT MAY CHANGE
Transient generated files, local service state, and non-canonical markdown content.

# WHAT IT MUST NOT CHANGE
Secrets, schema, database records, approved runtime architecture, or protected scripts.

# EXPECTED SAFE OUTPUT
Baseline status, preserved protected paths, and explicit skipped items.

# FAILURE BEHAVIOR
Stop before destructive action and return the blocking path or contract conflict.
