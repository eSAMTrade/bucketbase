---
type: "always_apply"
---

# Ground rules for the project (especially for Copilot and other AI tools, when generating code and tests):

## Project Overview
- **Domain**: Multi-language object storage abstraction library providing unified API for S3-compatible services (MinIO, AWS S3), filesystem, and in-memory backends
- **Architecture**: Interface-driven design (IBucket) with pluggable storage implementations across Python and Java runtimes​
- **Performance**: Correctness > readability > speed;

## Environment & Test Setup
- **Conda Environment**: `bucketbase` (Python 3.12+)
- **Working Directory**: Always `cd python/` before running tests
- **Test Framework**: unittest from stdlib ONLY; Don't use pytest, nose, etc.


## Test Completion Requirements
- **Unit Tests**: All tests must pass (typical runtime: 3-5 seconds)
- **Before Task Completion**: Always run both unit and integration tests to ensure no regressions
- **Test Coverage**: New code must have comprehensive unit tests covering all paths, edge cases, and error scenarios
- When generating code, an LLM agent must cover the code by unit-tests, ALL of the newly generated code;
  - For tests, you MUST strive cover all code paths, including edge cases;
  - test coverage should be meaningful and deterministic, avoiding sleeps and flaky tests.
  - Don't overlap the test logic, each test should test its own scenario;
  - Use boundary testing methods, find corner cases, and test them; Use partitioning equivalence principle to not generate redundant test;
  - Add negative test scenarios, and happy path scenarios; Strive to maximize the branch coverage;
- given we're mainly developing on windows, prefer scripts instead of `python -c "<multiline code>"` for running commands.
- when generating code or ideas, aim to reduce the code but improve maintainability and testability; respect design principles (DRY, SOLID, YAGNI, ..) and don't over-engineer; prefer simple, direct solutions over complex abstractions unless absolutely necessary(of if they would make for better maintainability and testability); avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).
- when generating code, prefer to use existing libraries and frameworks that are already used in the project, rather than introducing new ones unless absolutely necessary; prefer to use existing patterns and practices in the project, rather than introducing new ones unless absolutely necessary.
- after code generation, always review the generated code for compliance with the project's coding standards and guidelines, and ensure it adheres to the principles of maintainability, testability, and simplicity.
- after code generation, write a markdown file explaining the changes made, the reasoning behind them, and how they improve the codebase; include examples of how to use the new code, if applicable
- when generating code, don't over-engineer or over-abstract; prefer simple, direct solutions over complex abstractions unless absolutely necessary; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI); DRY and SOLID principles should be followed, but not at the cost of simplicity and maintainability; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).


## Coding standards
  - Readability & clarity: clear, unambiguous, self-documenting class/method names; focused docstrings without redundancy;
  - Variable naming: use precise, intention-revealing names; avoid ambiguous tuples or container names. Include the collection type and key/value intent when helpful (e.g., `users_by_id: Dict[int, User]`, `store_meta_by_name: dict[str, tuple[RawDataStore, DatasetMetadata]]`). Prefer plural for collections, singular for single values; avoid misleading names like store_and_meta for dicts. Names should describe what, not how.
  - Consistent attribute scope: decide visibility once and keep it consistent across base and subclasses. If a base declares _attr as internal, subclasses should not expose it as public attr, and vice versa. Don’t rebrand visibility through aliasing (e.g., self.public = self._internal); instead, provide explicit, documented accessors if needed.
  - Modularity & SRP: single-responsibility functions/classes; loose coupling; split over-loaded responsibilities; each class/method does one thing; favor small interfaces; decouple via Dependency Injection (avoid hard-wired new)
  - DRY & complexity: eliminate duplicated logic; simplify nested if/else; prefer Optional, Stream APIs or small helpers over deep nesting; reduce cyclomatic complexity;
  - Type hygiene & generics: no raw-types; use precise generic bounds; avoid unchecked casts and suppress-warnings;
  - Error handling & robustness: explicit exception flows; edge-case validation; no silent failures; catch specific exceptions; fail fast with meaningful messages; validate edge inputs with Objects.requireNonNull or custom checks
  - SOLID & extensibility: open for extension, closed for modification; favor interfaces over concrete types; avoid monolithic service classes;  easy to add features or swap implementations without invasive changes; Law of Demeter violations (tight coupling);
  - Pattern Selection: When suggesting pattern extraction, consider whether a template method pattern, strategy pattern, or other approach better preserves the contract and type safety than a generic helper method.
  - Abstraction Costs: Consider the trade-offs of each abstraction - does eliminating duplication introduce more complexity or coupling than it saves?
  - Method naming sanity check: verbs for actions, nouns for factories/getters; overloaded methods should differ clearly in intent, not just signature; names match behavior; suggest clearer alternatives where off
  - Avoiding False Equivalence: Don't assume similar-looking code should be merged if the methods have different conceptual responsibilities, return types, or error handling requirements.
  - Initialization & configuration: surface all required params in constructors or builders; objects should be initialized in valid states; avoid two-phase initialization, avoid hidden static initializers or global state; required public configuration is passed via public init parameters; internal wiring remains _internal. Do not surface internal attributes in the public signature.
  - Generic Abuse Check: Flag any use of generics that obfuscates the API contract or pushes errors from compile-time to runtime.
  - API noise & parameter hygiene: ditch catch-all Map<String,Object> or varargs flags; prefer explicit parameters or well-typed DTOs
  - Concurrency & performance: use thread-safe collections and immutables; avoid synchronized blocks where high-level abstractions suffice; watch for hot loops
  - Resource management: always close I/O in try-with-resources; no silent leaks
  - Initialization & configuration: all essential params surfaced in primary init/API; no hidden setup; objects should be initialized in valid states; avoid two-phase initialization;
  - API design principles: respect interface segregation; maintain class invariants; avoid implicit behaviors; follow principle of least surprise;
  - Parameter hygiene:  flag catch-all or overly flexible kwargs/parameters that add noise; recommend explicit scoped args
  - Security & Input Validation: check for injection risks, proper use of PreparedStatement, sanitization of user input, no hard-coded secrets.
  - Backward Compatibility & Versioning: flag any breaking API changes, missing deprecation notices, or hard-breaking refactors without migration paths.
  - Performance & Big-O Awareness: watch out for N² loops, unbounded collections, wasteful object creation in hot paths.
  - Resource & Memory Management: verify try-with-resources on all I/O, no unchecked thread pools, proper shutdown hooks.
  - Dispatch Mechanism Anti-patterns: flag uses of conditional logic (if/elif chains, match statements) to determine behavior based on object type; identify places where polymorphism could replace explicit type branching
  - Dependency Inversion Violations: identify places where high-level modules depend directly on concrete implementations instead of abstractions; check if code knows "how" rather than just "what"
  - Extensibility Design: verify if adding new variants/behaviors requires modifying existing code; check if extension points are defined through interfaces/abstract classes rather than conditional branches
  - Runtime Type Discrimination: flag patterns where code behavior changes based on runtime type checking; ensure behavior is encapsulated within objects rather than determined externally
  - Behavioral Contracts: ensure interface implementations truly fulfill the interface contract without special handling in client code; check for consistent patterns across related implementations
  - Factory Methods: evaluate if factories promote extensibility or create maintenance bottlenecks; ensure they adhere to the Open-Closed Principle by supporting new types without modification
  - Error Handling Recursion: Identify any error handlers that could trigger the same operation that failed, especially:
    -- Error messages that call methods/functions that previously failed; Side effects in exception handlers that might cause additional exceptions; Construction of error messages that could fail with the same error;
  - Exception Hygiene: Check for proper use of exception chaining with `from`, captured variables in error messages instead of live calls, and avoiding operations with side effects when formatting error messages.
  - Error Propagation: Verify that exceptions preserve the original cause and context, don't mask or transform the root cause unnecessarily, and include sufficient context for debugging.
  - Test coverage & quality: meaningful, focused unit tests; clear, deterministic assertions, no sleeps, no flacky tests;
  - Test access does not justify promotion: do not make internal members public just for tests; access internals via module-level helpers, fixtures, or test-only shims, or perform black-box testing.
  - PR Hygiene: check that the PR title and description clearly explain the "why" (not just the "what"), reference related tickets, and include before/after snippets where useful. Flag visibility drift: if a symbol changes visibility(or underscore prefix for py) in this PR (e.g., _foo → foo or foo → _foo), require an explicit rationale and update of all call sites, docs, and all.
  - Eliminate redundancy ruthlessly: Question the existence of EVERY class, wrapper, helper, and abstraction - if you cannot articulate a concrete, current benefit it provides beyond "future flexibility" or "just in case," flag it for removal; dead code paths, unused interfaces, wrapper classes that add zero value, and over-engineered abstractions are CODE SMELL violations that must be called out; prefer direct, simple solutions over complex hierarchies; if the same outcome can be achieved with fewer classes/methods/abstractions without sacrificing actual (not hypothetical) requirements, the simpler approach wins; **DEFAULT TO DELETION** - every line of code is a liability that must justify its existence with concrete, measurable value;


For each time, upon completion ensure the tests do pass. Ensure you run the correct test, you run them using the unittest python framework, and that they pass.
In some instructions you'll be told that you expect to run the tests. In those cases only run the tests, and don't fix them; In general you need to fix ALL of the tests before completion;

Upon completion ensure all of the tests do pass successfully; Ensure they really test what business logic requires and what makes sense;
Don't adjust the code just to pass the tests, ensure the code reflects business needs;

