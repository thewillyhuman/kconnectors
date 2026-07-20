# kconnectors

Monorepo of Kafka Connect connectors, built with [Bazel](https://bazel.build).
Every connector is an independent package: it has its own version, its own
Maven artifact, its own Kafka Connect plugin zip and its own release cadence.
CI only lints, tests and builds the connectors a change actually affects.

## Layout

```
connectors/
  kafka-connector-source-amq/   ActiveMQ (AMQP 1.0) -> Kafka source connector
    BUILD.bazel                 library + tests + plugin zip + maven export
    version.bzl                 the connector's version (single source of truth)
    src/main/java, src/test/java, src/main/resources
tools/
  ci/affected-connectors.sh     which connectors does a commit range affect?
  lint/pmd.sh, pmd-ruleset.xml  shared, blocking PMD lint
MODULE.bazel                    Bazel module + shared Maven dependency universe
maven_install.json              pinned Maven dependency lockfile
```

Connectors are named `kafka-connector-[source|sink]-<TYPE>`. The directory
name, Bazel package, Maven artifactId, plugin zip and release tags all use
that same name.

## Building

Install [bazelisk](https://github.com/bazelbuild/bazelisk) (`brew install
bazelisk`); it picks the Bazel version from `.bazelversion` and Bazel fetches
the JDK, so there is nothing else to install.

```bash
bazelisk test //...                                            # everything
bazelisk test //connectors/kafka-connector-source-amq/...      # one connector
bazelisk build //connectors/kafka-connector-source-amq:plugin  # Connect plugin zip
tools/lint/pmd.sh kafka-connector-source-amq                   # lint one connector
```

The plugin zip lands in
`bazel-bin/connectors/<name>/<name>-<version>-plugin.zip` and contains a
single self-contained jar (connector + runtime dependencies, minus what the
Connect runtime provides) — unpack it into the worker's `plugin.path`.

## CI: only what changed

`.github/workflows/ci.yml` first runs `tools/ci/affected-connectors.sh` on the
commit range, then fans out a lint job and a test/build job per affected
connector:

- `connectors/<name>/**` changed → only that connector runs.
- Shared files changed (`MODULE.bazel`, `maven_install.json`, `.bazelrc`,
  `tools/`, workflows) → every connector runs, because any of them could be
  affected.
- Top-level docs (`*.md`, `LICENSE`, …) → nothing runs.
- Unknown base (new branch, force push) → everything runs; over-building is
  safe, skipping is not.

Use the `ci ok` check for branch protection: it aggregates the dynamic matrix
into one required status.

## Releasing a connector

Versions live in each connector's `version.bzl` and releases are tags:

1. Bump `VERSION` in `connectors/<name>/version.bzl` (and merge to `main`).
2. Tag that commit `<name>-v<VERSION>` and push the tag:

   ```bash
   git tag kafka-connector-source-amq-v1.0.0
   git push origin kafka-connector-source-amq-v1.0.0
   ```

`.github/workflows/release.yml` refuses tags whose version does not match
`version.bzl`, re-runs the connector's tests, then publishes:

- the Maven artifact `io.github.thewillyhuman:<name>:<version>` (jar, sources,
  javadoc, pom) to this repository's GitHub Packages Maven registry, and
- a GitHub release with the Kafka Connect plugin zip attached.

## Consuming a connector as a Maven dependency

GitHub Packages requires authentication even for public packages: use a
personal access token with `read:packages`.

```xml
<repository>
  <id>kconnectors</id>
  <url>https://maven.pkg.github.com/thewillyhuman/kconnectors</url>
</repository>
```

```xml
<dependency>
  <groupId>io.github.thewillyhuman</groupId>
  <artifactId>kafka-connector-source-amq</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Adding a new connector

1. Create `connectors/kafka-connector-<source|sink>-<TYPE>/` with the standard
   `src/main/java`, `src/test/java` layout.
2. Add its Maven dependencies to `MODULE.bazel` and repin the lockfile:
   `REPIN=1 bazelisk run @unpinned_maven//:pin`.
3. Copy the shape of `connectors/kafka-connector-source-amq/BUILD.bazel`:
   a `version.bzl`, a `java_export` named like the connector, a
   `java_test_suite`, and the `plugin` zip targets.
4. That is all: CI picks the directory up automatically, and the connector
   releases with `<name>-v<version>` tags.

Dependency policy: one shared, pinned Maven universe for the whole repo
(`maven_install.json`), so two connectors never disagree about a version;
isolation between connectors happens at the Bazel target level.
