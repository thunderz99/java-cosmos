# Repository Guidelines

## Project Structure & Module Organization
- Core sources live under `src/main/java/io/github/thunderz99/cosmos`, split into `dto`, `impl`, `util`, and database-specific helpers such as `v4`.
- Public test fixtures sit in `src/test/java/io/github/thunderz99/cosmos` with resource data in `src/test/resources`.
- Supporting assets include `docs/` for published examples, `mongo/` for local integration materials, and helper scripts `install.sh` and `deploy.sh` for Maven workflows.
- Shared build metadata is defined in `pom.xml` and `settings.xml`; keep updates to these files tightly reviewed.

## Build, Test, and Development Commands
- `mvn clean verify` runs compile, unit tests, and packaging—use this before sharing change sets.
- `mvn test` executes the JUnit 5 suite only; prefer this for fast local checks.
- `./install.sh` installs the artifact into your local Maven repository using the custom `settings.xml`.
- `./deploy.sh` publishes to the configured Maven repository; run only after release approval.

## Coding Style & Naming Conventions
- Target Java 17; rely on Maven’s compiler plugin and keep source files encoded in UTF-8.
- Use four-space indentation, `UpperCamelCase` for classes, `lowerCamelCase` for members, and keep package names lowercase, mirroring `io.github.thunderz99.cosmos`.
- Favor fluent builder patterns already present (e.g., `CosmosBuilder`) and keep new utilities colocated with existing peers (`impl`, `util`, or `condition`).
- Run your IDE formatter before committing; align imports with the current ordering and avoid wildcard imports.

## Testing Guidelines
- Tests use JUnit Jupiter with AssertJ; place new cases beside the related component and follow the `ClassNameTest` naming pattern.
- Create focused unit tests for each scenario, exercising both Cosmos DB and Mongo/PostgreSQL paths when logic diverges.
- Use deterministic fixtures under `src/test/resources`; avoid external service calls unless guarded behind environment checks.
- New behavior should ship with tests that fail pre-change; update or extend mocks rather than mutating production code for testing.

## Commit & Pull Request Guidelines
- Follow the Conventional Commits style visible in history (`feat:`, `fix:`, etc.), optionally appending issue references like `(#194)`.
- Each commit should remain scoped to a logical change; avoid bundling unrelated refactors.
- Pull requests need a concise summary, validation notes (commands run, databases used), and any screenshots or log excerpts that clarify the effect.
- Link to the relevant issue or discussion and confirm CI status before requesting review; rerun `mvn clean verify` if rebasing.

## Security & Configuration Tips
- Never commit secrets; load connection strings through environment variables or `.env` files consumed by `dotenv-java`.
- Store publication credentials in `~/.m2/settings.xml` or use `settings.xml` templates checked into `docs/` as guidance.
- Scrub logs before attaching them to issues to prevent leaking keys or tenant data.
