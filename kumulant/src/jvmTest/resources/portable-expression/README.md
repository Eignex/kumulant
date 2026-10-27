# Portable expression conformance

`conformance.json` is copied byte for byte from Klause's matching JVM resource.
It is self-authored and records the portable expression profile documented in
`schema/expr/package.md` and Klause's input semantics. Change both copies together.

Each case includes the expression, scalar bindings, canonical encoding where needed,
and integer and binary64 outcomes. Kumulant runs the binary64 cases, rejects unsupported
operations and shapes, and retains nested grouping. `NaN`, `Infinity`, and `-Infinity`
are evaluation outcomes, not JSON literals.
