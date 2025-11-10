---
type: "always_apply"
---

Python specifics:
  - Dependencies & config: minimal third-party libs; externalize settings; no hidden constants
  - Style & conventions: PEP8/PEP257 compliance; 160 char max line length; project lint rules;
  - Logging & Observability: ensure use of structured logging (logging module), correct log levels, flag any print() statements used for logging purposes, and hooks for metrics or tracing.
  - Allowed popular libs: pandas, numpy, dask, polars, streams, pyxtension, tsx
    -- where possible, prefer Java stream style (using streams library) over the `for` loops. Prefer using `tsx` over ANY datatime types.
  - By default, expect Python 3.12 compatibility, if not stated otherwise in PR.
  - Type hygiene: complete and correct type hints; no implicit Any
    -- Getter Methods: Don't flag well-documented getter/accessor methods that have clear purposes as violations of encapsulation when they're providing intentional access to internal state.
  - Type-Safe Refactoring: Avoid introducing ambiguous return types (Any) or runtime casting. Maintain precise type signatures when refactoring methods. Don't sacrifice static type checking for DRY.
  - Return Type Clarity: Methods should have clear, specific return types. Flag any solution that mixes return types based on string parameters or other runtime values.
  - Visibility & naming (Python): adopt consistent underscore prefixes to signal intent, not enforce access. Public: no underscore (stable API). Protected-internal: single underscore _name (internal to module/package or subclass use; subject to change). Private-to-class: double underscore __name (name-mangled; only when you must prevent accidental override in subclasses). Don’t mix scopes for the same symbol across hierarchy.
  - Attribute scope clarity: don’t declare mutable class attributes that are later shadowed by per-instance assignments; explicitly model intent with either instance attributes initialized in init/setUp or class attributes marked ClassVar without instance rebinding. Prefer factory/abstract methods to make initialization points explicit. Use clear names without leading underscores unless truly non-public.
    -- Pick one scope and stick to it: If a base class exposes state to subclasses, model it as protected instance state (single underscore) initialized in the base, or as a protected factory method the base calls. Do not define a mutable class attribute and then reassign it per-instance in subclasses.
    -- No shadowing class -> instance: Mutable class attributes that are later rebound on instances are forbidden. If you need per-test values, move the value to an instance attribute initialized in __init__/setUp, or provide it via a protected factory (_create_*) that subclasses override.
    -- Dependency injection over mutation: If the value is a dependency, prefer passing it into the base’s constructor/setUp or overriding a factory method; don’t mutate a base attribute from a subclass.
  - Readability & clarity:
    -- No inner functions (nested defs) for core logic; Prefer top-level or class-level helpers for shared logic. Allowed only when:
    --- Used in tests (unit or integration tests)
        --- Implementing a decorator or higher-order function where the wrapper must close over parameters;
        --- Creating a short callback passed immediately to a single API (e.g., key functions, small predicates) where a lambda would be too cramped but a top-level function would be noise.
        --- A closure is required to bind a small amount of immutable context without introducing a class or partial.

## Common Test Issues & Solutions
- **File paths**: Use `Path` objects, handle Windows paths correctly
- **Timing**: Use `tsx.iTS` for timestamps, not Python datetime
- **Memory**: Integration tests use temporary directories, clean up properly
- **Before Task Completion**: Always run both unit and integration tests to ensure no regressions
- When generating code, an LLM agent must cover the code by unit-tests, ALL of the newly generated code;
  - For tests, you MUST strive cover all code paths, including edge cases;
  - test coverage should be meaningful and deterministic, avoiding sleeps and flaky tests.
  - Don't overlap the test logic, each test should test its own scenario;
  - Test public API only, not private methods/members. Hard-to-test private methods = refactor signal (extract to new class).
  - Use boundary testing methods, find corner cases, and test them; Use partitioning equivalence principle to not generate redundant test;
  - Add negative test scenarios, and happy path scenarios; Strive to maximize the branch coverage;
- given we're mainly developing on windows, prefer scripts instead of `python -c "<multiline code>"` for running commands.
- when generating code or ideas, aim to reduce the code but improve maintainability and testability; respect design principles (DRY, SOLID, YAGNI, ..) and don't over-engineer; prefer simple, direct solutions over complex abstractions unless absolutely necessary(of if they would make for better maintainability and testability); avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).
- when generating code, prefer to use existing libraries and frameworks that are already used in the project, rather than introducing new ones unless absolutely necessary; prefer to use existing patterns and practices in the project, rather than introducing new ones unless absolutely necessary.
- after code generation, always review the generated code for compliance with the project's coding standards and guidelines, and ensure it adheres to the principles of maintainability, testability, and simplicity.
- after code generation, write a markdown file explaining the changes made, the reasoning behind them, and how they improve the codebase; include examples of how to use the new code, if applicable
- when generating code, don't over-engineer or over-abstract; prefer simple, direct solutions over complex abstractions unless absolutely necessary; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI); DRY and SOLID principles should be followed, but not at the cost of simplicity and maintainability; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).

Terminal/command run instructions:
You are running over powershell!

Use `pip` for package management, and `poetry` or `pipenv` for dependency management in projects. These are expected to be available from command line.
Python: we use Python 3.12+ on our projects, and we use `conda` for environments. This project has conda environment, which is expected to be activated on the cmd line.
Try to perform as much as possible changes at a time. Don't do small iterations - do bigger iterations. Run tests only after the code is done for all of the task, and check then what works or not. Never adapt the tests to the code - follow the business logic based on the Class/Method names and comments, and write the tests regardless of what you read in the code. Decide the logic only based on interfaces/high-level namings in the code - not the actual existing logic there.
Project context: energy HFT trading. Performance matters, but readability and correctitude matters even more! Follow the best practices from Python industry, write clean and maintainable code following PEP 8, use type hints, and leverage tools like black, isort, and mypy. Reach internet if needed for search.
Don't upgrade or downgrade the Python versions or libraries unless this is explicitly needed.

- When writing tests, use `unittest` stdlib and follow best practices from QA like partitioning equivalence, boundary testing, orthogonal arrays, and think also about corner cases and negative scenarios. Be careful also with the unit-tests running time, to be lightning fast, think of every wasted millisecond, avoid time.sleep() where possible, use pytest fixtures and mocking appropriately.
- Avoid using prints or logging in the tests, try to generate clean code, and without comments. Code should be self explanatory, and comments only in places where it's explicitly needed.
- Please use the instructions from `/.github/copilot-instructions.md` from the root of the project, for the better context;
- Ensure that the tests you write also pass by running them, but only at the end, avoid excessive test running to save time, and develop the task faster.
- Don't introduce new project dependencies, unless this is the only way to do stuff, or this would lead to lot of redundant code. If you have a suggestion of adding a dependency library which would solve issues, please suggest and ask this first.
- Prefer `tsx`(https://github.com/asuiu/tsx) library  over other datetime Python classes. This is the de-facto standard in our projects for high-precision timing. Analyze the exports of tsx before using.
- For operations over collections/iterators, prefer Fluent pattern (using streamerate lib (https://github.com/asuiu/streamerate) );
- In general, before taking a decision on what class to use, take a look over the other modules and if there's a class doing what you need, use that instead of common Python libs.
- If you change code somewhere, at the end of development, ensure the tests in the whole project also pass. Avoid excessive test-running, but never finish the dev task without ensuring the tests pass.
- If the existing test uses some external files/data, and now it's missing - don't generate new synthetic data - but fail the task and notify the user that the data is missing and you need explicit confirmation to ignore that and generate another data;
- NEVER-EVER remove or move the existing files in the repository. You can do operations only on the files YOU have created. Be very cautious with the existing files. Change only the code, and don't run move commands in the terminal,
	never run dangerous commands in shell or terminal! Don't touch external files, which are not created explicitly by you! Better ask before performing actions!
- Remember, NEVER-EVER remove or move the existing files in the repository!
- NEVER skip/disable the tests, even if the data is missing; When you consider thta the input data or connection to an external resource is missing, just notify me about this, DON't skip test;
- Remember, NEVER skip/disable the tests !
- when need to access the Web/Internet for documentation, use the connected Fetcher MCP or PlayWright;
- For short/long term memory of documentation/documents, use the Server Memory MCP;
- use Context7 for documentation; use Sequential Thinking to plan the tasks you do;


- This project uses unittest from stdlib(only) for testing.
- `# mypy: disable-error-code="no-untyped-def"` can (is advised to) be used to disable mypy errors for untyped functions ONLY in test files.
- When generating code, an LLM agent must strive to add test coverage for the generated code. when generating tests, it must strive to cover all code paths, including edge cases; test coverage should be meaningful and deterministic, avoiding sleeps and flaky tests.
- given we're mainly developing on windows, prefer scripts instead of `python -c "<multiline code>"` for running commands.
- when generating code or ideas, aim to reduce the code but improve maintainability and testability; respect design principles (DRY, SOLID, YAGNI, ..) and don't over-engineer; prefer simple, direct solutions over complex abstractions unless absolutely necessary(of if they would make for better maintainability and testability); avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).
- when generating code, prefer to use existing libraries and frameworks that are already used in the project, rather than introducing new ones unless absolutely necessary; prefer to use existing patterns and practices in the project, rather than introducing new ones unless absolutely necessary.
- after code generation, always review the generated code for compliance with the project's coding standards and guidelines, and ensure it adheres to the principles of maintainability, testability, and simplicity.
- after code generation, write a markdown file explaining the changes made, the reasoning behind them, and how they improve the codebase; include examples of how to use the new code, if applicable
- when generating code, don't over-engineer or over-abstract; prefer simple, direct solutions over complex abstractions unless absolutely necessary; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI); DRY and SOLID principles should be followed, but not at the cost of simplicity and maintainability; avoid unnecessary complexity and abstractions that don't provide concrete, current value (YAGNI).

