# Publishing the Java library to Maven Central

This repository publishes the Java artifact as:

```text
com.esamtrade:bucketbase:<release-version>
```

**The POM is release-ready.** All prior blockers are fixed (§2): MIT license, correct GPG key
with loopback pinentry, current plugin versions, release version `0.1.0`, and clean consumer
dependencies. The remaining steps are the account actions that need your login: generate a
Central Portal token (§3), then publish (§6 or §7). Run §4 → §5 first as a final check.

---

## 0. Verified facts (checked 2026-07-22)

These were verified against the live services and the local machine — treat them as known
good, not as things to re-do.

| Item | Status |
|---|---|
| Namespace `com.esamtrade` | **Verified & working.** `com.esamtrade:aicodesign:0.1.0` is live on Central (published 2026-06-27), which is only possible with a verified namespace. No DNS step is needed. |
| `com.esamtrade:bucketbase` | **No versions published.** Any version number is still free, including `0.1.0` and `1.0.0`. |
| Signing key | Present in the local GnuPG keyring as `sec rsa4096/F2C8FE56BBBF7F0D` `[SCEAR]`, ultimate trust. |
| Primary fingerprint | `26366396DA3F1ECE2CEE662FF2C8FE56BBBF7F0D` |
| Signing subkey | `104E54DFC0F15F333050EC5517747811CB523EF6` |
| Key UID | `eSAMTrade (Maven Central signing key for com.esamtrade) <billing@esamtrade.com>` |
| Public key on keyserver | **Already published** to `keyserver.ubuntu.com`. No re-upload needed. |
| `.publish-secrets/` | Contains `private-key.asc`, `public-key.asc`, `gpg_passphrase.txt`. Gitignored — never commit, print, or paste the contents of these files. |

The only thing still missing for authentication is a **Central Portal user token** (§3).

> The stale Gradle output in `java/build/libs/` (`bucketbase-1.0.0*.jar`) is from an earlier
> abandoned Gradle setup. Nothing there was ever published. Delete the directory; the build
> is Maven-only now.

---

## 1. Current build status

```bash
cd java && /path/to/mvn -B -ntp clean test
```

Result as of 2026-07-23: **BUILD SUCCESS** — `Tests run: 98, Failures: 0, Errors: 0`.

The suite includes 24 tests that run against a **live S3-compatible service**. Endpoint and
credentials come from `TestConfig`, which defaults to the public MinIO playground
(`https://play.min.io`, bucket `bucketbase-test`) so a release build needs no private secrets.
Override with `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` /
`MINIO_DEV_TESTS_BUCKET` to target a private instance, or set
`BUCKETBASE_SKIP_FUNCTIONAL_TESTS=1` to skip them entirely.

A release build **must** run the functional tests — publish only what a fully green build
produced, and never release with `-DskipTests`. A published artifact is immutable.

Note that surefire now sets `--add-modules jdk.incubator.vector`, required by the test-scoped
`trino-parquet` dependency. Test scope only: the published artifact has no Parquet dependency.

---

## 2. POM status — all release blockers fixed

Every item below is already done in the current `pom.xml`. Listed for the record.

| Was | Now |
|---|---|
| `commons-io` was `provided` (→ consumer `NoClassDefFoundError` on `PurePosixPath`) | `compile` |
| `commons-codec` imported but undeclared | Removed entirely — the code needing it (a hand-rolled MinIO `Content-MD5` delete fallback) was fragile and deleted |
| Full `com.amazonaws:aws-java-sdk` pulled in (every AWS service) | Slimmed to `aws-java-sdk-s3` |
| `<version>0.1.0-SNAPSHOT</version>` | `0.1.0` (Central rejects SNAPSHOTs) |
| `<licenses>` said Apache 2.0 | **MIT**, matching the repo `LICENSE` |
| GPG `<keyname>pypi@esamtrade.com</keyname>` (a UID not in the keyring → signing fails) | fingerprint `26366396DA3F1ECE2CEE662FF2C8FE56BBBF7F0D` + `--pinentry-mode loopback` |
| `maven-gpg-plugin` 3.1.0 | 3.2.7 |
| `central-publishing-maven-plugin` 0.7.0 | 0.11.0, `autoPublish` + `waitUntil=published` |
| `maven-source-plugin` 3.3.0 goal `jar` | 3.3.1 goal `jar-no-fork` |
| `maven-javadoc-plugin` 3.5.0 | 3.11.2, `doclint=none` |
| `source`/`target` 17 on a JDK 25 toolchain | `<maven.compiler.release>25</maven.compiler.release>` |
| Second `<developer>` `esamtrade-pypi` | removed |

Both the source jar and javadoc jar build cleanly (verified). The consumer dependency tree is:
`commons-io` (compile), AWS SDK v2 `s3` + AWS SDK v1 `aws-java-sdk-s3` (both **optional**), and
JUnit/Trino (**test**). A core-only consumer inherits nothing but `commons-io`.

**AWS SDKs are optional**, so document in the README that an S3 consumer must add
`software.amazon.awssdk:s3` (for `S3Bucket`) or `com.amazonaws:aws-java-sdk-s3` (for
`S3BucketSDKv1`) themselves. **Java 25** is the required runtime — state it prominently.

The signing and publishing plugin config already lives in `pom.xml`. The GPG passphrase is read
from the `MAVEN_GPG_PASSPHRASE` environment variable by default. **Never** pass
`-Dgpg.passphrase=...` — Maven documents that command-line passphrases leak through shell
history and `/proc` process inspection.

> For a first, cautious release you may temporarily set `<autoPublish>false</autoPublish>` in the
> `central-publishing-maven-plugin` config to review the deployment in the Portal before it goes
> live; the current default is `true` (auto-release once validation passes).

---

## 3. One-time Central Portal setup

Only step 3 below is still outstanding; steps 1–2 are already satisfied (§0).

1. ~~Verify the `com.esamtrade` namespace~~ — already verified.
2. ~~Publish the public signing key to a keyserver~~ — already on `keyserver.ubuntu.com`.
3. **Generate a Central Portal user token.** Sign in at <https://central.sonatype.com/> with
   the eSAMTrade account → account page → *Generate User Token*. Copy the generated
   **username** and **password**; the password is shown once. Your normal account password is
   not a publishing credential.

Official plugin documentation:
<https://central.sonatype.org/publish/publish-portal-maven/>.

---

## 4. Release gate

Before changing the version or creating a tag:

1. Fix the remaining items in §2.
2. Confirm the Java API is actually the API you want to freeze. `getSize` and `openWrite` now
   exist, but see `ASSESSMENT.md` for what is still missing — notably the filesystem-backed
   bucket and the fact that `removeObjects` cannot report failures. Released coordinates are
   immutable and the public API is hard to change afterwards.
3. Run the full test suite, including the credentialed MinIO integration tests, and confirm
   it is green.
4. Build a throwaway consumer project against `mvn install` output (see §2.1).
5. Build with JDK 25, the declared target.
6. Confirm the version is not already on Central:
   <https://central.sonatype.com/artifact/com.esamtrade/bucketbase>.

---

## 5. Validate locally without publishing

Requires JDK 25, Maven, and GnuPG. The signing key is already imported on this machine; on a
fresh machine import it once:

```bash
gpg --import .publish-secrets/private-key.asc
gpg --list-secret-keys --keyid-format long 26366396DA3F1ECE2CEE662FF2C8FE56BBBF7F0D
```

Prime `gpg-agent` interactively so the passphrase never enters shell history:

```bash
gpg --local-user 26366396DA3F1ECE2CEE662FF2C8FE56BBBF7F0D --clearsign
# type anything, then Ctrl-D; enter the passphrase when prompted
```

Then:

```bash
cd java
mvn -B -ntp clean verify
```

Check that `target/` contains, for the release version: the main JAR, `-sources.jar`,
`-javadoc.jar`, the POM, and an `.asc` signature **for each**. Then inspect what a consumer
would actually resolve:

```bash
mvn -B -ntp dependency:tree
mvn -B -ntp help:effective-pom
```

---

## 6. Publish manually from a workstation

Add the Central token to `~/.m2/settings.xml`. **This file already exists on this machine
with four `github-*` servers — merge, do not overwrite it.**

```xml
<settings>
    <servers>
        <!-- keep the existing github-* servers -->
        <server>
            <id>central</id>
            <username>CENTRAL_TOKEN_USERNAME</username>
            <password>CENTRAL_TOKEN_PASSWORD</password>
        </server>
    </servers>
</settings>
```

After §4 and §5 pass:

```bash
cd java
export MAVEN_GPG_PASSPHRASE="$(cat ../.publish-secrets/gpg_passphrase.txt)"
mvn -B -ntp deploy
unset MAVEN_GPG_PASSPHRASE
```

With `autoPublish=true` and `waitUntil=published`, Maven blocks until Central finishes
publishing. Without `autoPublish`, Maven waits only for validation and you must release the
deployment manually under **Publish → Deployments** in the Portal.

Confirm afterwards at
<https://central.sonatype.com/artifact/com.esamtrade/bucketbase>. Availability on
`repo1.maven.org` and in search typically lags publication by 15–30 minutes.

---

## 7. Publish from GitHub Actions

This repository has **no Java publishing workflow yet** (`.github/workflows/` contains only
`python-package.yml`).

⚠️ **Tag namespace collision.** The Python package already owns the `v*` tag namespace —
`python/pyproject.toml` sets commitizen `tag_format = "v$version"` and the package is at
`1.7.1`. A `v*`-triggered Java workflow would fire on every Python release. Use a distinct
prefix such as **`java-v*`**.

Create `.github/workflows/publish-java.yml`:

```yaml
name: Publish Java to Maven Central

on:
  push:
    tags:
      - 'java-v*'
  workflow_dispatch:

permissions:
  contents: read

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 25
        uses: actions/setup-java@v4
        with:
          java-version: '25'
          distribution: 'temurin'
          server-id: central                      # matches <publishingServerId> in pom.xml
          server-username: CENTRAL_TOKEN_USERNAME
          server-password: CENTRAL_TOKEN_PASSWORD
          gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}
          gpg-passphrase: MAVEN_GPG_PASSPHRASE

      - name: Publish to Maven Central
        working-directory: java
        run: mvn -B -ntp deploy
        env:
          CENTRAL_TOKEN_USERNAME: ${{ secrets.CENTRAL_TOKEN_USERNAME }}
          CENTRAL_TOKEN_PASSWORD: ${{ secrets.CENTRAL_TOKEN_PASSWORD }}
          MAVEN_GPG_PASSPHRASE: ${{ secrets.GPG_PASSPHRASE }}
          MINIO_ACCESS_KEY: ${{ secrets.MINIO_ACCESS_KEY }}
          MINIO_SECRET_KEY: ${{ secrets.MINIO_SECRET_KEY }}
```

Required repository secrets (**Settings → Secrets and variables → Actions**):

| Secret | Value |
|---|---|
| `CENTRAL_TOKEN_USERNAME` | username from the Central user token (§3) |
| `CENTRAL_TOKEN_PASSWORD` | password from the Central user token (§3) |
| `GPG_PRIVATE_KEY` | full contents of `.publish-secrets/private-key.asc`, including the BEGIN/END lines |
| `GPG_PASSPHRASE` | the single line in `.publish-secrets/gpg_passphrase.txt` |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | optional — only to run the functional tests against a private instance instead of the public playground |

Note the indirection `actions/setup-java` uses: `gpg-passphrase:` takes the **name of the
environment variable** the passphrase will be exposed as, not the passphrase itself. That
name must be `MAVEN_GPG_PASSPHRASE`, which is what `maven-gpg-plugin` reads by default.

Release only after confirming the POM version exactly matches the tag:

```bash
git tag java-v0.1.0
git push origin java-v0.1.0
```

Never move or reuse a release tag after a publication attempt.

---

## 8. Troubleshooting

| Symptom | Cause |
|---|---|
| `gpg: signing failed: Inappropriate ioctl for device` | Missing `--pinentry-mode loopback`, or `MAVEN_GPG_PASSPHRASE` unset. |
| `gpg: skipped "...": No secret key` | `<keyname>` does not match a key in the keyring — the current POM's `pypi@esamtrade.com` hits exactly this. |
| Central rejects the bundle: *missing signature / javadoc / sources* | The `verify` phase did not run all three plugins. Never publish from `mvn package`. |
| Central rejects: *invalid version* | A `-SNAPSHOT` version. |
| `401 Unauthorized` from the portal | Using the account password instead of a generated user token, or a `<server>` id other than `central`. |
| `409 Conflict` / *version already exists* | Central releases are immutable. Bump the version; you cannot overwrite. |
| `NoClassDefFoundError: org/apache/commons/io/FilenameUtils` in a consumer | The `provided` scope on `commons-io` (§2.1) was not fixed. |

---

## 9. Fallback: GitHub Packages

Not recommended for a public library — every consumer must authenticate with a GitHub PAT
just to download the JAR. Use only if Central is unavailable: add a `<distributionManagement>`
block pointing at `https://maven.pkg.github.com/eSAMTrade/bucketbase`, add a matching
`<server>` to `~/.m2/settings.xml` with a `write:packages` PAT, and run `mvn deploy`.
