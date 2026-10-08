"""Internal helpers shared by every SDK layer: guarded JSON parsing,
resource-close sweeps, and single-value lazy initialization. A leaf: it
imports no layered package, so every layer can depend on it."""
