# browser-extension-kit

Source-only TypeScript package of Effect helpers for Chrome extensions
(Manifest V3). Consumers compile the shipped `.ts`.

## Stack

- TypeScript strict (`verbatimModuleSyntax`, `isolatedModules`)
- Effect v3 (not v4)
- Vitest + `@effect/vitest`
- pnpm; conventional commits `type: emoji description` (no Co-authored-by)

## Craft

- No `any`. No type assertions (`as`, non-null `!`). Narrow with Schema or guards.
- Validate unknown data at boundaries. Prefer schema-derived types.
- File names: kebab-case. Errors are tagged values, not throws or magic strings.
- Effect pipe-first. Prefer typed errors, composability, and Layer/Context DI.
- Relative imports inside this package (no path aliases in shipped source).

## Layering

- `chrome.*` globals belong only in infrastructure adapters (live layers).
- Higher layers depend on services/Context tags, never on `chrome` directly.
- Subpath exports: `browser-extension-kit/messaging`, `browser-extension-kit/testing`.
  Keep fakes and test helpers under `/testing`.

## Tests

- Unit: pure logic, no real Chrome.
- Integration: in-memory fakes from `/testing`; deterministic clocks where timing matters.
- No live accounts, cookies, or credentials in CI.

## Scope discipline

- Keep this package generic. No consumer product names, host-site specifics,
  or private planning material in source, tests, or docs.
