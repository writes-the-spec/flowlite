---
name: code-style
description: General code style rules for this repo - simplicity, readability, formatting, and when redundancy is preferable to abstraction. Use whenever writing or reviewing Rust code here, not just when adding a specific feature.
---

# Code style

These rules apply to every file in this repo, on top of (not instead of) any entity-specific skill (`crud`, `cli`, `entities`).

## Simple and human-readable over clever

- Write code the way you'd explain it out loud. Prefer the obvious, boring solution over a clever one-liner or a generic abstraction, even if the clever version is shorter.
- Optimize for the next person reading the code cold, not for minimizing keystrokes.
- Don't introduce a trait, generic, macro, or helper function to unify two or three call sites unless they are actually going to change together. If they're conceptually independent, let them look independent in the code.

## Redundancy is fine when it reads better

- Duplicated code is not automatically a problem. If deduplicating two similar blocks would require a new abstraction, an extra parameter threading through unrelated logic, or a name that has to describe two different things at once, keep the duplication.
- A rule of thumb: three similar lines repeated in a few places is better than one shared helper that everyone has to open to understand what each call site actually does.
- Only extract a shared function/const when the duplication represents one real concept that must stay in sync (e.g. a magic value, a validation rule, a query fragment used identically everywhere) — not just similar-looking code.

## Formatting

- Break up long lines and long chains (`.method().method().method()`, long `if`/`&&` conditions, long argument lists) across multiple lines rather than letting them run wide. A reader shouldn't have to scroll horizontally or lose track of a condition partway through.
- Give each logical step its own line/block with a blank line between unrelated steps, instead of packing multiple actions into one dense line.
- Prefer named intermediate variables over deeply nested expressions when a nested expression would otherwise require re-reading to parse.
- Match the formatting already present in the surrounding file rather than introducing a new personal style within one module.

## Comments: few, and clean

- Aim for **minimal comments**. Well-named functions and variables are the primary documentation — if a comment is needed to explain *what* a line does, rename things or split the function instead of writing the comment.
- The comments worth keeping explain the **why**: a non-obvious constraint, a deliberate ordering, a coordination rule between two pieces of code that don't reference each other. That kind of context can't be read off the code.
- A short doc comment on a type or public function whose *role* isn't obvious from its name is fine — one or two lines, describing responsibility, not implementation.
- Don't restate the code (`// increment the counter`), don't leave TODO/placeholder chatter, and don't leave commented-out code in place — delete it; git has it.
- Keep comments accurate when the code changes. A stale comment is worse than none, which is another reason to have few of them.

## When these conflict with "don't over-engineer"

Simplicity beats DRY, but simplicity also beats needless repetition of something that is genuinely one concept (e.g. don't copy-paste a SQL WHERE-clause-builder loop three times — that's the same idea three times, not three different ideas). Use judgment: ask whether merging the duplicates would make an individual call site *harder* to read in isolation. If yes, keep them separate.
