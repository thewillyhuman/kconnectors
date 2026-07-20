# Agent guide

Monorepo of Kafka Connect connectors built with Bazel. Every connector is an
independent package with its own version, Maven artifact, plugin zip and
release cadence. CI only lints, tests and builds the connectors a change
actually affects.

## Layout

```
connectors/<name>/            one connector per directory
  BUILD.bazel                 library + tests + plugin zip + maven export
  version.bzl                 the connector's version (single source of truth)
  src/main/java, src/test/java, src/main/resources
tools/ci/affected-connectors.sh   change detection used by CI
tools/lint/pmd.sh                 blocking PMD lint (shared ruleset)
MODULE.bazel                  Bazel module + shared Maven dependency universe
maven_install.json            pinned Maven lockfile
```

## Conventions

- Connector names: `kafka-connector-[source|sink]-<TYPE>`. The directory,
  Bazel package, Maven artifactId, plugin zip and release tag all use it.
- Java packages: `io.github.thewillyhuman.kafka.connect.[source|sink].<type>`.
- Maven groupId: `io.github.thewillyhuman`.
- One shared, pinned dependency universe for the whole repo: add artifacts to
  `MODULE.bazel`, then repin with `REPIN=1 bazelisk run @unpinned_maven//:pin`.
  Connector isolation happens at the Bazel target level.
- Dependencies provided by the Connect runtime (connect-api, kafka-clients,
  slf4j-api) are compile deps but are excluded from the plugin zip via
  `deploy_env`.

## Build, test, lint

Only bazelisk is required; it pins Bazel via `.bazelversion` and Bazel
downloads the JDK.

```bash
bazelisk test //...                          # everything
bazelisk test //connectors/<name>/...        # one connector
bazelisk build //connectors/<name>:plugin    # Kafka Connect plugin zip
tools/lint/pmd.sh <name>                     # lint (blocking in CI)
```

The plugin zip lands in
`bazel-bin/connectors/<name>/<name>-<version>-plugin.zip`.

## CI and releases

- `.github/workflows/ci.yml` computes affected connectors from the commit
  range (`connectors/<name>/**` -> that connector; shared files -> all;
  top-level docs -> none) and fans out lint and test/build jobs per
  connector. `ci ok` is the aggregate check.
- Releases are per connector: bump `VERSION` in `version.bzl`, merge, then
  push an annotated tag `<name>-v<VERSION>`. The release workflow refuses
  tags that disagree with `version.bzl`, re-tests, publishes the Maven
  artifact to this repository's GitHub Packages registry and attaches the
  plugin zip to a GitHub release.

## How to commit

- Commits are atomic: one logical change, and undoing the commit must leave
  the project in a consistent state.
- Title: max 50 characters, always lowercase, descriptive, imperative, with
  a conventional type prefix (`feat:`, `fix:`, `build:`, `ci:`, `docs:`, ...).
- Body: wrapped at 72 characters. Explain why the change was needed, how it
  was implemented (motivate the decisions taken) and how it was tested.
  Complete but not verbose.
- Trailers, and nothing else:
  - `Assisted-by:` the current AI model, when one assisted
    (e.g. `Assisted-by: Claude Fable 5`).
  - `Signed-off-by:` the human git committer.
- Do not add `Co-Authored-By` or `Claude-Session` trailers.
