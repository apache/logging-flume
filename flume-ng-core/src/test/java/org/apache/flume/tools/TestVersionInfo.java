/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Properties;
import java.util.jar.Attributes;
import org.junit.Test;

public class TestVersionInfo {

    private static final String PURL = "pkg:maven/org.apache.flume/flume-ng-core@2.0.0";

    private static final String BUNDLE_SCM = "url=\"https://gitbox.apache.org/repos/asf/logging-flume.git\","
            + "connection=\"scm:git:https://gitbox.apache.org/repos/asf/logging-flume.git\","
            + "developer-connection=\"scm:git:https://gitbox.apache.org/repos/asf/logging-flume.git\","
            + "tag=\"rel/2.0.0\"";

    /**
     * Checks the metadata of the artifact the test runs against.
     *
     * <p>BND writes the manifest to the output directory before the tests run, so the values are
     * available whether Flume is loaded from a JAR or from the compiled classes.
     */
    @Test
    public void testMetadataOfOwnArtifact() {
        assertTrue(
                "getVersion returned " + VersionInfo.getVersion(),
                VersionInfo.getVersion().matches("\\d+\\.\\d+.*"));
        assertTrue(
                "getPurl returned " + VersionInfo.getPurl(),
                VersionInfo.getPurl().startsWith("pkg:maven/org.apache.flume/flume-ng-core@"));
        assertTrue(
                "getUrl returned " + VersionInfo.getUrl(),
                VersionInfo.getUrl().startsWith("https://")
                        && VersionInfo.getUrl().contains("logging-flume"));
        assertTrue(
                "getBranch returned " + VersionInfo.getBranch(),
                VersionInfo.getBranch().startsWith("rel/"));
        // Throws if the timestamp is not ISO-8601.
        Instant.parse(VersionInfo.getDate());
        assertTrue(
                "getBuildVersion returned " + VersionInfo.getBuildVersion(),
                VersionInfo.getBuildVersion().matches(".+ from .+ built on .+"));
    }

    @Test
    @SuppressWarnings({"deprecation", "removal"})
    public void testUnrecordedMetadata() {
        assertEquals("Unknown", VersionInfo.getRevision());
        assertEquals("Unknown", VersionInfo.getUser());
        assertEquals("Unknown", VersionInfo.getSrcChecksum());
    }

    @Test
    public void testVersionPrefersTheManifest() {
        Attributes manifest = new Attributes();
        manifest.putValue("Implementation-Version", "2.0.0");
        assertEquals("2.0.0", VersionInfo.version(manifest, pomProperties("1.11.0")));
    }

    @Test
    public void testVersionFallsBackToPomProperties() {
        assertEquals("1.11.0", VersionInfo.version(new Attributes(), pomProperties("1.11.0")));
    }

    @Test
    public void testVersionWithoutAnySource() {
        assertEquals("Unknown", VersionInfo.version(new Attributes(), new Properties()));
    }

    @Test
    public void testPurlPrefersTheManifest() {
        Attributes manifest = new Attributes();
        manifest.putValue("Purl", PURL);
        assertEquals(PURL, VersionInfo.purl(manifest, pomProperties("1.11.0")));
    }

    @Test
    public void testPurlIsBuiltFromPomProperties() {
        assertEquals(
                "pkg:maven/org.apache.flume/flume-ng-core@1.11.0",
                VersionInfo.purl(new Attributes(), pomProperties("1.11.0")));
    }

    @Test
    public void testPurlWithoutAnySource() {
        assertEquals("Unknown", VersionInfo.purl(new Attributes(), new Properties()));
    }

    /** A manifest of an artifact Flume was shaded into must not be mistaken for our own. */
    @Test
    public void testForeignManifestIsRejected() {
        Attributes foreign = new Attributes();
        foreign.putValue("Purl", "pkg:maven/com.example/uber-jar@1.0.0");
        foreign.putValue("Implementation-Version", "1.0.0");
        assertFalse(VersionInfo.isOwn(foreign));
        assertFalse(VersionInfo.isOwn(new Attributes()));

        Attributes own = new Attributes();
        own.putValue("Purl", PURL);
        assertTrue(VersionInfo.isOwn(own));
    }

    @Test
    public void testScmAttributes() {
        assertEquals("rel/2.0.0", VersionInfo.scmAttribute(BUNDLE_SCM, "tag"));
        assertEquals(
                "https://gitbox.apache.org/repos/asf/logging-flume.git", VersionInfo.scmAttribute(BUNDLE_SCM, "url"));
        assertEquals(
                "scm:git:https://gitbox.apache.org/repos/asf/logging-flume.git",
                VersionInfo.scmAttribute(BUNDLE_SCM, "developer-connection"));
        assertNull(VersionInfo.scmAttribute(BUNDLE_SCM, "revision"));
        assertNull(VersionInfo.scmAttribute(null, "tag"));
    }

    /** OSGi only requires quoting for values with special characters. */
    @Test
    public void testScmAttributeWithoutQuotes() {
        assertEquals("rel/2.0.0", VersionInfo.scmAttribute("url=https://example.org,tag=rel/2.0.0", "tag"));
    }

    private static Properties pomProperties(String version) {
        Properties properties = new Properties();
        properties.setProperty("groupId", "org.apache.flume");
        properties.setProperty("artifactId", "flume-ng-core");
        properties.setProperty("version", version);
        return properties;
    }
}
