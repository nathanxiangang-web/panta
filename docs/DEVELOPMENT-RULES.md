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
