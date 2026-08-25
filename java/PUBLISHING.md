# Publishing the Java library to Maven Central

The Gradle project publishes:

```text
com.esamtrade:bucketbase:<release-version>
```

The first version is `0.1.0`. Override it for a release with `-PreleaseVersion=<version>`.
Maven Central releases are immutable, and an upload task refuses a version ending in `-SNAPSHOT`.

## Build and publication design

- Gradle Wrapper: `9.6.1`
- Java toolchain and bytecode release: `25`
- Central Portal integration: `com.vanniktech.maven.publish` `0.37.0`
- Automatic release: enabled
- Deployment validation: waits until `PUBLISHED`
- Signing: in-memory ASCII-armored OpenPGP key

Every Central upload task depends on the complete `test` task. Publishing cannot bypass the
test suite.

The generated consumer POM contains:

- `commons-io` as a runtime dependency;
- AWS SDK v2 `s3` as an optional dependency;
- AWS SDK v1 `aws-java-sdk-s3` as an optional dependency.

JUnit and Trino are test-only and do not appear in the production JAR or consumer POM.

## Verified local state

Verified on 2026-07-24, and unchanged since:

- the Gradle 9.6.1 distribution SHA-256 matched Gradle's published checksum;
- the Gradle Wrapper JAR SHA-256 matched Gradle's published checksum.

Re-verified on 2026-08-25, after `FSBucket` was added:

- all 156 Java tests passed, run in parallel in one JVM, including live MinIO tests through AWS SDK v1 and v2;
- the main, sources and Javadoc JARs were generated, and the main JAR contains `FSBucket`;
- the Maven POM contains the required license, developer and SCM metadata;
- all JAR, POM and Gradle-module signatures were generated and verified with GPG.

## Release history

Check this list before choosing a version, and verify it against Central itself with
`curl -s -o /dev/null -w '%{http_code}' https://repo1.maven.org/maven2/com/esamtrade/bucketbase/<version>/`.
A released version can never be reused.

| Version | Released | Contents |
|---|---|---|
| `0.1.0` | 2026-07-24 | First release. No `FSBucket`. |
| `0.1.1` | 2026-08-25 | Adds `FSBucket`, the local-filesystem backend. |

Both are live on Maven Central. An earlier revision of this file claimed no component had ever
been uploaded; that was wrong for `0.1.0` and led to `0.1.0` being offered as a free version.

## Prerequisites

1. JDK 25 must be installed.
2. The `com.esamtrade` namespace must be verified in the
   [Central Portal](https://central.sonatype.com/).
3. Generate a Central Portal user token. Its generated username and password—not the account
   password—are the publishing credentials.
4. The signing public key must be available from a public keyserver.
5. The repository-root `.publish-secrets/` directory must contain:
   - `private-key.asc`
   - `gpg_passphrase.txt`

The existing `.publish-secrets/` directory is ignored by Git. Never commit or print its contents.

## Normal build

From the `java/` directory:

```bash
./gradlew --no-daemon --non-interactive clean test
```

To skip the live MinIO tests during ordinary development:

```bash
BUCKETBASE_SKIP_FUNCTIONAL_TESTS=1 \
    ./gradlew --no-daemon --non-interactive clean test
```

Do not skip functional tests for a release.

The suite runs concurrently inside a single JVM: test classes in parallel with each other, and the
methods inside each class in parallel too. `src/test/resources/junit-platform.properties` holds the
settings and the reasoning. The full suite takes about 30 seconds this way, against roughly four
minutes of serial test time.

This means several tests work against the configured MinIO bucket at the same time. Each one keys
its objects under a private UUID prefix, so they do not collide. Point `MINIO_DEV_TESTS_BUCKET` at a
bucket you are willing to have written to concurrently.

## Validate release artifacts without uploading

Build the three release JARs and generate the POM. `assemble` builds only the main and sources
JARs, so name `plainJavadocJar` explicitly:

```bash
./gradlew --no-daemon --non-interactive \
    clean test assemble plainJavadocJar generatePomFileForMavenPublication
```

Inspect:

```text
build/libs/bucketbase-<version>.jar
build/libs/bucketbase-<version>-sources.jar
build/libs/bucketbase-<version>-javadoc.jar
build/publications/maven/pom-default.xml
```

Load the local signing files into Gradle environment properties:

```bash
export ORG_GRADLE_PROJECT_signingInMemoryKey="$(<../.publish-secrets/private-key.asc)"
export ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="$(<../.publish-secrets/gpg_passphrase.txt)"
```

Generate signatures locally:

```bash
./gradlew --no-daemon --non-interactive signMavenPublication
```

The signatures are written next to the three JARs and under
`build/publications/maven/`. Verify them with `gpg --verify` before publishing.

Dry-run the complete upload and automatic-release graph:

```bash
./gradlew --no-daemon --non-interactive \
    publishAndReleaseToMavenCentral --dry-run
```

Unset the signing properties when finished:

```bash
unset ORG_GRADLE_PROJECT_signingInMemoryKey
unset ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
```

## Publish

Set the Central token without placing it in `gradle.properties` or shell command arguments:

```bash
read -r -p "Central token username: " CENTRAL_TOKEN_USERNAME
read -r -s -p "Central token password: " CENTRAL_TOKEN_PASSWORD
echo

export ORG_GRADLE_PROJECT_mavenCentralUsername="$CENTRAL_TOKEN_USERNAME"
export ORG_GRADLE_PROJECT_mavenCentralPassword="$CENTRAL_TOKEN_PASSWORD"
unset CENTRAL_TOKEN_USERNAME CENTRAL_TOKEN_PASSWORD

export ORG_GRADLE_PROJECT_signingInMemoryKey="$(<../.publish-secrets/private-key.asc)"
export ORG_GRADLE_PROJECT_signingInMemoryKeyPassword="$(<../.publish-secrets/gpg_passphrase.txt)"
```

Confirm that the selected version is unused, then publish:

```bash
./gradlew --no-daemon --non-interactive \
    -PreleaseVersion=0.1.2 \
    publishAndReleaseToMavenCentral
```

The task:

1. runs the full Java test suite;
2. builds the main, sources and Javadoc JARs;
3. generates Maven and Gradle metadata;
4. signs the publication;
5. uploads it to the Central Portal;
6. requests release automatically;
7. waits until the deployment reaches `PUBLISHED`.

Always remove the credentials from the environment afterwards:

```bash
unset ORG_GRADLE_PROJECT_mavenCentralUsername
unset ORG_GRADLE_PROJECT_mavenCentralPassword
unset ORG_GRADLE_PROJECT_signingInMemoryKey
unset ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
```

Confirm the release at:

```text
https://central.sonatype.com/artifact/com.esamtrade/bucketbase
```

Central search and `repo1.maven.org` can lag behind the `PUBLISHED` state.

## Consumer dependencies

Core, in-memory and local-filesystem use:

```groovy
implementation "com.esamtrade:bucketbase:<version>"
```

`MemoryBucket` and `FSBucket` need no backend SDK.

AWS SDK v2 backend:

```groovy
implementation "com.esamtrade:bucketbase:<version>"
implementation "software.amazon.awssdk:s3:2.30.36"
```

AWS SDK v1 backend:

```groovy
implementation "com.esamtrade:bucketbase:<version>"
implementation "com.amazonaws:aws-java-sdk-s3:1.12.782"
```

The AWS SDK dependencies are intentionally optional; a core-only consumer does not download
either SDK.

## CI secret names

For a future GitHub Actions publishing workflow, configure:

- `CENTRAL_TOKEN_USERNAME`
- `CENTRAL_TOKEN_PASSWORD`
- `GPG_PRIVATE_KEY`
- `GPG_PASSPHRASE`
- optional private MinIO credentials

Use the distinct Java tag namespace `java-v*`; the Python package already uses `v*`.

Map the secrets to:

```text
ORG_GRADLE_PROJECT_mavenCentralUsername
ORG_GRADLE_PROJECT_mavenCentralPassword
ORG_GRADLE_PROJECT_signingInMemoryKey
ORG_GRADLE_PROJECT_signingInMemoryKeyPassword
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Missing Maven Central credentials` | The generated Central token username/password were not exposed through the two `mavenCentral*` Gradle properties. |
| `Cannot perform signing task ... no configured signatory` | The in-memory private key or its password is missing. |
| Central reports missing sources, Javadocs or signatures | Publish with `publishAndReleaseToMavenCentral`; do not manually upload only `build/libs/*.jar`. |
| Central rejects the POM | Inspect `build/publications/maven/pom-default.xml` and confirm all required metadata is present. |
| `409 Conflict` or an already-existing version | Central releases cannot be overwritten; choose a new version. |
| S3 classes fail to compile in a consumer | Add the matching optional AWS SDK dependency shown above. |
