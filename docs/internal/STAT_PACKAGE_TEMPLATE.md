# Kumulant style guide

## Stat package.md template

Every stat subpackage (`com.eignex.kumulant.stat.<family>`) carries a
`package.md` that Dokka renders as the family landing page. This is the
family-level companion to the per-class [STAT_KDOC_TEMPLATE](STAT_KDOC_TEMPLATE.md):
the KDoc template governs one stat, this template governs the page that orients
a reader across the whole family and sends them to the right member.

A family page answers four questions in order: *what is this family*, *which
member do I pick*, *how do the members relate*, and *how do they behave under
merge and concurrency*. The sections below map one-to-one onto those questions.

```markdown
# Package com.eignex.kumulant.stat.<family>

<Lead paragraph: two to four sentences. What the family computes, the shared
input modality (link the core Stat interface), and the common output trait or
result shape that unifies the members.>

## Picking a <member-noun>

<Selection table, one row per user-facing stat. First column is always the
stat; the middle column(s) are the discriminators that decide the choice
(memory, input shape, result, accuracy regime); the last column is
"Reach for it when ...".>

## <Relationship / per-member prose>   (optional, repeatable)

<H2 sections that explain how members relate, give the algorithm/recurrence,
or document non-obvious per-member knobs. Skip for small families whose
selection table already says enough.>

## Result shapes   (optional)

<Table mapping each result type to its shape. Include when members return
different result types; skip when they share one.>

## Compose patterns   (optional)

<Recurring compositions built from this family rather than as dedicated stats.>

## Merge

<Which members merge exactly, which approximate, which don't merge at all and
what to ship instead. One paragraph or a bullet per member.>

## Concurrency

<The dominant update mechanism and per-Concurrency behaviour across the
family. Cross-reference the per-stat KDoc for member-specific detail.>
```

### Required and optional sections

Always present, in this order:

1. **Title** — `# Package com.eignex.kumulant.stat.<family>`, the fully
   qualified package name, nothing else on the line.
2. **Lead paragraph** — no header. What the family computes, the shared input
   modality, the unifying output trait or result shape.
3. **Selection section** — `## Picking a <member-noun>` with a selection
   table. See *Selection-heading naming* below.
4. **`## Merge`** — the family's merge story.
5. **`## Concurrency`** — the family's concurrency story.

Optional, between the selection section and `## Merge`:

- **Relationship / per-member prose** — one or more H2 sections naming how the
  members relate, the recurrence or paper behind a member, or constructor
  knobs. Skip when the selection table is self-explanatory.
- **`## Result shapes`** — a result-type table. Include only when members
  return different result types.
- **`## Compose patterns`** — recurring compositions over the family.

### Selection-heading naming

The selection section heading reads `## Picking a <member-noun>`, where the
noun names what the family contains: `Picking a detector`, `Picking an
estimator`, `Picking a model`, `Picking a rate estimator`. This is the most
common existing form and the one to converge on.

Two documented exceptions, for families that are not "pick one of N":

- A **family root** that re-exports subfamilies (e.g.
  `com.eignex.kumulant.stat.regression`, `com.eignex.kumulant.stat`) uses
  `## What's in the family` / `## Family map` and a navigation table instead.
- A family whose page is **organised by sub-group rather than by single
  pick** (e.g. `summary`'s trivial / Welford / robust groupings) keeps its
  grouped H2 sections in place of one flat table, but still closes with
  `## Merge` and `## Concurrency`.

### Section style

- `## H2` for every section heading; never `#` (reserved for the title) and
  never bare bold labels (those are the per-class KDoc convention, not the
  package-page one).
- Name the closing sections exactly `## Merge` and `## Concurrency` — not
  "Merge support", "Merge story", or "Memory and concurrency at a glance".
  Grep-ability across families is the point.
- Selection and result tables lead with the `[Stat]` / `[Result]` column so
  the Dokka cross-links line up down the left edge.
- Declarative, not narrative. The page orients and routes; depth lives in the
  per-class KDoc.
- Link sibling stats with `[StatName]` and cross-package references with the
  fully qualified `[com.eignex.kumulant....]` form, matching the KDoc style.
