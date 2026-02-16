# AGENTS.md

## 1. Scope
This file defines repository-specific guidance for autonomous agents.  
System behavior, architecture, and data-processing mechanics are documented in `README.md`.

## 2. Working Boundaries
- Treat `raw_data/` and `parquet_data/` as data directories.
- Do not modify data files unless the task explicitly requires it.
- Prioritize source files in `src/` and test files in `tests/`.
- When documentation and implementation diverge, implementation is the source of truth.

## 3. Project Entry Points for Agents
- Pipeline orchestration: `src/main.rs`
- Indicator definitions: `src/indicators.rs`
- Timeframe aggregation: `src/resampler.rs`
- Stitching logic: `src/stitcher.rs`
- Input loading: `src/loader.rs`
- Parquet persistence: `src/storage.rs`

## 4. Change Workflow
1. Read the relevant module implementations before editing documentation.
2. Update `README.md` whenever behavior, constants, schema, or execution flow changes.
3. Keep `README.md` implementation-accurate; remove stale statements.
4. Run targeted checks after code changes:
   - `cargo check`
   - `cargo test --lib` (or narrower test selection when applicable)
5. Report any failing tests and the reason in the final handoff.

## 5. Documentation Maintenance Standard
Apply the following standard for all documentation updates in this repository.

### Tone and Voice
- Use formal, neutral, technical language.
- Use third-person, impersonal voice.
- Avoid conversational or expressive wording.
- Avoid promotional or subjective language.

### Purpose
Documentation must:
- describe system behavior and mechanics
- document architecture and component responsibilities
- define constants, constraints, and assumptions
- specify execution flow and state transitions
- support maintenance and implementation changes

Documentation must not:
- promote the project
- include narrative storytelling
- include opinion-based justification
- include vague statements without technical detail

### Writing Style
- Prefer concise, declarative sentences.
- State what the system does and how it behaves.
- Remove filler text and rhetorical phrasing.
- Replace generic descriptions with concrete implementation details.

### Structure and Formatting
- Organize by clear headings and logical sections.
- Use tables and bullet lists where they improve precision.
- Remove redundant or duplicated content.
- Resolve contradictions instead of preserving alternatives.
- Remove obsolete or speculative sections.

### Accuracy Rules
- Do not invent behavior.
- Prefer current implementation details over prior documentation.
- If behavior is uncertain, inspect code and tests before writing.
- If implementation appears inconsistent, document observed behavior and note it as a constraint.

## 6. Documentation Split Policy
- `README.md`: primary technical reference for system behavior, architecture, schemas, runtime parameters, and execution logic.
- `AGENTS.md`/`Agents.md`: instructions for agent workflow, editing process, validation expectations, and documentation standards.

## 7. Pre-Handoff Checklist
- Updated docs match current source code.
- Added/renamed indicators are reflected in schema and indicator sections.
- Time/session boundaries in docs match `src/indicators.rs`.
- File naming and output conventions match `src/main.rs`.
- Outdated sections removed or rewritten.
