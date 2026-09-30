# Panta — Development & Acceptance Rules

## 1. Team model

Panta MVP uses two roles.

### Architect
Owns:
- product/architecture boundaries;
- contracts and module dependency direction;
- gate scope;
- acceptance criteria;
- review and final gate acceptance;
- changes to project context/state.

The Architect does not expand scope mid-gate without updating the blueprint/state first.

### Developer
Owns:
- implementation of the currently authorized task;
- tests;
- migrations;
- operational notes needed to run the change;
- acceptance evidence;
- documentation updates caused by the implementation.

The Developer does not redesign neighboring modules without an architect decision.

## 2. Git is project memory

Chat is not the source of truth.

Every material project decision must eventually exist in Git as one of:
- blueprint/context update;
- state update;
- decision record;
- contract;
- implementation/tests;
- acceptance evidence.

At the end of a gate, `PROJECT-STATE.md` must say what is actually true.

### 2.1 Source-of-truth precedence

When project sources disagree, use:

```text
newer explicit ACCEPTED decision
    > PROJECT-STATE current pointer
    > current code/contracts/tests
    > PROJECT-CONTEXT / MVP-BLUEPRINT
    > old Issue / PR text
    > chat memory
```

Do not silently choose between contradictions. Record the conflict, make an explicit Architect decision, and synchronize the affected Git memory documents.

### 2.2 Mandatory AI preflight

Before designing, authorizing, implementing, or reviewing a new Gate, the Architect/AI must read:

```text
docs/AI-ARCHITECTURE-MEMORY.md
PROJECT-STATE.md
newest relevant entries in docs/DECISIONS.md
current Issue / PR
actual current code/contracts
```

If an external repository is involved, inspect its current accepted interface before writing the implementation Issue.

Before proposing a new module/integration path, search Git for an already accepted owner of that responsibility.

This preflight is mandatory specifically to prevent long-context drift, stale assumptions, duplicate responsibilities, and AI hallucination.

## 3. Development unit

One task should be small enough for one developer to implement and verify without simultaneously changing unrelated modules.

Each task description should include:
- Background
- Goal
- Allowed scope
- Forbidden scope
- Inputs/contracts
- Expected outputs
- Tests
- Acceptance evidence

If a task fails twice for the same underlying reason, stop blind retries and write a short failure record using:

```text
Background:
Goal:
Execution:
Result:
Root cause / next decision:
```

## 4. Branch / review discipline

Recommended flow:

```text
main
 ↑
PR
 ↑
bounded feature/fix branch
```

Rules:
- one bounded concern per PR;
- no unrelated refactors in feature PRs;
- tests/evidence travel with the code;
- architect accepts the gate only after reviewing actual behavior, not only code presence;
- main should remain deployable/testable after every merge.

## 5. Module dependency rules

Core domain must depend on ports/contracts, not provider implementations.

Allowed direction:

```text
API / jobs / application
        ↓
     domain
        ↓
 ports / contracts
        ↑
 adapters / integrations
```

Provider implementation may depend on provider libraries/MCP.
Domain may not import `providers/115`.

IndexCore/OpenList/CodeArts integrations are adapters. They are not domain models.

Observation ownership is stricter than ordinary adapter layering:

```text
Storage -> OpenList -> IndexCore -> Panta
```

IndexCore owns OpenList collection/scoped verification and canonical physical truth. Panta acquisition must not introduce a second direct OpenList verification/scanning lane. Panta may send trusted Mutation Hints to IndexCore and then consume IndexCore Query/Journal evidence.

An OpenList integration in Panta is permitted only for a separately authorized access-layer/product need; it must never become a second canonical observation source.

## 6. Side-effect rule

All provider-changing operations must be expressed as jobs/commands.

Examples:
- cloud download;
- move/rename;
- share creation/revocation;
- future provider mutations.

A controller/agent must not perform an untracked side effect behind the Job Engine.

## 7. Evidence rule

Gate acceptance should include evidence appropriate to the stage:
- unit tests;
- contract tests;
- integration tests;
- real service test when the gate claims real provider behavior;
- failure/restart test when durability is claimed;
- measured timestamps when “fast synchronization” is claimed.

Do not turn observations into SLA claims unless an SLA is explicitly tested.

## 8. Low-coupling acceptance tests

Before MVP closeout, the codebase must prove:
1. core domain builds/tests with 115 implementation removed or disabled;
2. a mock provider implements the same provider contracts;
3. search can be disabled without breaking direct path/browse/acquisition;
4. agent runtime can be disabled without breaking deterministic core flows;
5. OpenList/IndexCore outage produces explicit degraded/failure states instead of invented success.

## 9. Scope-control rule

The current gate wins over “nice to have.”

Do not prebuild:
- distributed infrastructure for hypothetical scale;
- generalized plugin ecosystems before the second real provider;
- advanced UI before flows work;
- agent orchestration that duplicates the Job Engine.

## 10. Gate handoff format

At the end of each development stage, the Developer reports:

```text
Implemented:
Tests:
Evidence:
Known limitations:
Docs changed:
Out-of-scope items untouched:
Recommended next gate:
```

Architect response is one of:
- ACCEPTED
- ACCEPTED WITH FOLLOW-UP
- REJECTED / FIX REQUIRED

Only ACCEPTED (or an explicit architect exception) advances the project.


## 11. Architecture-memory synchronization rule

A Gate is not fully closed from the Architect's perspective until project memory is synchronized when necessary.

Synchronize at least:
- `docs/AI-ARCHITECTURE-MEMORY.md`;
- `PROJECT-STATE.md`;
- `docs/DECISIONS.md` when a boundary/contract changes;
- `PROJECT-CONTEXT.md` when durable architecture truth changes;
- `docs/MVP-BLUEPRINT.md` when the end-to-end flow changes;
- README when its overview would otherwise contradict current truth.

A correction that supersedes an accepted decision must be explicit and append-only in `docs/DECISIONS.md`; do not erase the historical decision and pretend it never existed.

Before handing work to the Developer, the current Issue must match the synchronized architecture memory.

## 12. Hallucination stop conditions

Stop and re-read Git instead of continuing from memory if any of these occur:
- the proposed flow introduces a new owner for work already assigned to another module;
- the proposed path bypasses an accepted canonical-truth boundary;
- two coordinates/IDs are being derived from one another without an accepted mapping;
- a historical Issue is being treated as more authoritative than a later accepted decision;
- a module is being introduced because “it seems needed” without locating its owner in the current architecture;
- the worker count/team topology differs from the accepted 1 Architect + 1 Developer model;
- the next Gate is being planned before the current Gate is accepted and merged.

When triggered, the Architect must cite the conflicting Git sources internally, resolve the boundary, update project memory, and only then continue.
