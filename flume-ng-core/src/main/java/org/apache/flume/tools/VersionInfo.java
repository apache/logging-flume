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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * Provides the build metadata of the Flume artifact this class was loaded from.
 *
 * <p>The values come from the manifest of that artifact, falling back to its Maven descriptor when
 * Flume has been shaded into another artifact and the manifest is no longer its own. Every accessor
 * returns {@value #UNKNOWN} rather than {@code null} when the information is unavailable.
 */
public class VersionInfo {

    private static final String UNKNOWN = "Unknown";

    private static final String GROUP_ID = "org.apache.flume";
    private static final String ARTIFACT_ID = "flume-ng-core";

    private static final String IMPLEMENTATION_TIMESTAMP = "Implementation-Timestamp";
    private static final String PURL = "Purl";
    private static final String BUNDLE_SCM = "Bundle-SCM";

    private static final String PURL_PREFIX = "pkg:maven/" + GROUP_ID + "/" + ARTIFACT_ID + "@";
    private static final String CLASS_PATH = "org/apache/flume/tools/VersionInfo.class";
    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";
    private static final String POM_PROPERTIES_PATH =
            "/META-INF/maven/" + GROUP_ID + "/" + ARTIFACT_ID + "/pom.properties";

    /** Matches one {@code name=value} pair of an OSGi header, with an optionally quoted value. */
    private static final Pattern SCM_ATTRIBUTE =
            Pattern.compile("(?:^|,)\\s*([A-Za-z0-9_-]+)\\s*=\\s*(?:\"([^\"]*)\"|([^,]*))");

    private static final Attributes MANIFEST = ownManifest();
    private static final Properties POM_PROPERTIES = pomProperties();

    private static final String VERSION = version(MANIFEST, POM_PROPERTIES);
    private static final String PURL_VALUE = purl(MANIFEST, POM_PROPERTIES);
    private static final String URL_VALUE = orUnknown(scmAttribute(MANIFEST.getValue(BUNDLE_SCM), "url"));
    private static final String TAG = orUnknown(scmAttribute(MANIFEST.getValue(BUNDLE_SCM), "tag"));
    private static final String DATE = orUnknown(MANIFEST.getValue(IMPLEMENTATION_TIMESTAMP));

    /**
     * Reads the manifest of the artifact this class was loaded from.
     *
     * <p>Resolving it against the location of this class, instead of looking up
     * {@code META-INF/MANIFEST.MF} on the class path, keeps another artifact from answering. Returns
     * empty attributes when the manifest is missing or belongs to an artifact Flume was shaded into.
     */
    private static Attributes ownManifest() {
        URL self = VersionInfo.class.getResource("VersionInfo.class");
        if (self == null) {
            return new Attributes();
        }
        String location = self.toString();
        if (!location.endsWith(CLASS_PATH)) {
            return new Attributes();
        }
        String root = location.substring(0, location.length() - CLASS_PATH.length());
        try (InputStream stream = URI.create(root + MANIFEST_PATH).toURL().openStream()) {
            Attributes attributes = new Manifest(stream).getMainAttributes();
            return isOwn(attributes) ? attributes : new Attributes();
        } catch (IOException | RuntimeException ignored) {
            return new Attributes();
        }
    }

    private static Properties pomProperties() {
        Properties properties = new Properties();
        try (InputStream stream = VersionInfo.class.getResourceAsStream(POM_PROPERTIES_PATH)) {
            if (stream != null) {
                properties.load(stream);
            }
        } catch (IOException | RuntimeException ignored) {
            // Falls through to the empty properties.
        }
        return properties;
    }

    /** Tells whether the manifest describes this artifact rather than one Flume was shaded into. */
    static boolean isOwn(Attributes manifest) {
        String purl = manifest.getValue(PURL);
        return purl != null && purl.startsWith(PURL_PREFIX);
    }

    static String version(Attributes manifest, Properties pomProperties) {
        String version = manifest.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
        return orUnknown(StringUtils.isNotBlank(version) ? version : pomProperties.getProperty("version"));
    }

    static String purl(Attributes manifest, Properties pomProperties) {
        String purl = manifest.getValue(PURL);
        if (StringUtils.isBlank(purl)) {
            String groupId = pomProperties.getProperty("groupId");
            String artifactId = pomProperties.getProperty("artifactId");
            String version = pomProperties.getProperty("version");
            if (groupId != null && artifactId != null && version != null) {
                purl = "pkg:maven/" + groupId + "/" + artifactId + "@" + version;
            }
        }
        return orUnknown(purl);
    }

    /**
     * Returns one attribute of an OSGi {@code Bundle-SCM} header, or {@code null} if absent.
     *
     * <p>The header is specified by OSGi Core R8, section 3.2.1, as a comma separated list of
     * {@code url}, {@code connection}, {@code developer-connection} and {@code tag} attributes.
     */
    static String scmAttribute(String header, String attribute) {
        if (header == null) {
            return null;
        }
        Matcher matcher = SCM_ATTRIBUTE.matcher(header);
        while (matcher.find()) {
            if (attribute.equals(matcher.group(1))) {
                String quoted = matcher.group(2);
                return quoted != null ? quoted : matcher.group(3).trim();
            }
        }
        return null;
    }

    private static String orUnknown(String value) {
        return StringUtils.defaultIfBlank(value, UNKNOWN);
    }

    /**
     * Gets the Flume version.
     *
     * @return the Flume version string, eg. "2.0.0"
     */
    public static String getVersion() {
        return VERSION;
    }

    /**
     * Gets the Package URL of the Flume artifact this class was loaded from.
     *
     * @return the Package URL, eg. "pkg:maven/org.apache.flume/flume-ng-core@2.0.0"
     */
    public static String getPurl() {
        return PURL_VALUE;
    }

    /**
     * Gets the source control revision this was built from.
     *
     * <p>The build no longer records a commit id, since it has to produce the same artifact from a
     * Git checkout and from the source distribution, which carries no repository metadata.
     *
     * @return always "Unknown"
     */
    public static String getRevision() {
        return UNKNOWN;
    }

    /**
     * Gets the source control tag or branch this was built from.
     *
     * @return the tag, eg. "rel/2.0.0"
     */
    public static String getBranch() {
        return TAG;
    }

    /**
     * Gets the date Flume was built.
     *
     * @return the build date in ISO-8601 format, or "Unknown" if unavailable
     */
    public static String getDate() {
        return DATE;
    }

    /**
     * Gets the user that compiled Flume.
     *
     * @return always "Unknown"
     * @deprecated Recording the user would make the build unreproducible.
     */
    @Deprecated(since = "2.0.0", forRemoval = true)
    public static String getUser() {
        return UNKNOWN;
    }

    /**
     * Gets the source control URL of the Flume repository.
     *
     * @return the repository URL
     */
    public static String getUrl() {
        return URL_VALUE;
    }

    /**
     * Gets the checksum of the source files Flume was built from.
     *
     * @return always "Unknown"
     * @deprecated Use {@link #getPurl()} to identify the artifact, and verify it against the
     *     checksums published with the release.
     */
    @Deprecated(since = "2.0.0", forRemoval = true)
    public static String getSrcChecksum() {
        return UNKNOWN;
    }

    /** Returns the build version info, which includes the version, the tag and the build date. */
    public static String getBuildVersion() {
        return getVersion() + " from " + getBranch() + " built on " + getDate();
    }

    public static void main(String[] args) {
        System.out.println("Flume " + getVersion());
        System.out.println("Package URL: " + getPurl());
        System.out.println("Source code repository: " + getUrl());
        System.out.println("Tag: " + getBranch());
        System.out.println("Compiled on " + getDate());
    }
}
