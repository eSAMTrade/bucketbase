# Bucketbase

## Instructions to build and publish a release

### Prerequisites
- An account on central.sonatype.com has to be created using instructions at https://central.sonatype.org/register/central-portal/#create-an-account
_Important Note_: enable SNAPSHOT publishing after defining the namespace.
- Create authentication tokens on central.sonatype.com and save them in ~/.mvn/settings.xml:
```xml

<settings>
    <servers>
        <server>
            <id>central</id>
            <username>PICK FROM ZOHO</username>
            <password>PICK FROM ZOHO</password>
        </server>
    </servers>
</settings>
```
_Important note_ The authentication tokens were regenerated so remember to update the above.
- GPG Signatures: This one-time-only step was already performed 
  - create a keypair
  ``gpg --gen-key``
  - publish the public key: ``gpg --keyserver keys.openpgp.org --send-keys AAAB0592065F7ED7BC852DABAC39F2FDF53EE367``. Remember to confirm the publishing via email link.
- Picking the key pair from ZOHO, saved under pypi@esamtrade.com Password Custom Fields
- Define credentials in .mvn/settings.xml to allow tests to run with minio credentials:
```xml
<settings>
    <profiles>
        <profile>
            <id>default</id>
            <properties>
                    <MINIO_ACCESS_KEY>minio-dev-tests</MINIO_ACCESS_KEY>
                    <MINIO_SECRET_KEY>PICK FROM ZOHO KEY minio-dev-tests@minio</MINIO_SECRET_KEY>
            </properties>
        </profile>
    </profiles>

    <activeProfiles>
        <activeProfile>default</activeProfile>
    </activeProfiles>
</settings>

```
### Adjustments in pom.xml
In case of multiple keys being locally defined, you need to indicate we want to sign with pypi@esamtrade.com
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-gpg-plugin</artifactId>
    <version>3.1.0</version>
    <executions>
        <execution>
            <id>sign-artifacts</id>
            <phase>verify</phase>
            <goals>
                <goal>sign</goal>
            </goals>
            <configuration>
                <keyname>pypi@esamtrade.com</keyname>
            </configuration>
        </execution>
    </executions>
</plugin>
```
