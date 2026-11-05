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
package org.apache.flume.source;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.sdk.test.TestKeyStores;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSslContextAwareAbstractSource {

    private static final String KEYSTORE_PASSWORD = "password";

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    private Path keystore;

    /** Minimal concrete source exposing the SSL configuration of the abstract class. */
    private static final class TestSource extends SslContextAwareAbstractSource {
        void configure(Context context) {
            configureSsl(context);
        }
    }

    @Before
    public void writeKeystore() throws Exception {
        keystore = TestKeyStores.selfSigned("CN=localhost")
                .writeKeyStore(tempFolder.newFile("keystore.jks").toPath(), "JKS", KEYSTORE_PASSWORD);
    }

    private Context sslContext(String password) {
        Context context = new Context();
        context.put("ssl", "true");
        context.put("keystore", keystore.toString());
        context.put("keystore-password", password);
        return context;
    }

    private TestSource configuredSource() {
        TestSource source = new TestSource();
        source.configure(sslContext(KEYSTORE_PASSWORD));
        return source;
    }

    @Test
    public void sslContextIsCreatedFromKeystore() {
        TestSource source = configuredSource();
        assertTrue(source.isSslEnabled());
        assertTrue(source.getSslContextSupplier().get().isPresent());
        assertTrue(source.getSslEngineSupplier(false).get().isPresent());
    }

    @Test
    public void noSslContextWhenSslIsDisabled() {
        TestSource source = new TestSource();
        source.configure(new Context());
        assertFalse(source.isSslEnabled());
        assertFalse(source.getSslContextSupplier().get().isPresent());
    }

    @Test(expected = FlumeException.class)
    public void wrongKeystorePasswordIsRejected() {
        new TestSource().configure(sslContext("wrong"));
    }

    @Test
    public void keystoreIsDeletableAfterUse() {
        // The source must not keep the keystore open: on Windows an open file cannot be deleted
        assertTrue(configuredSource().getSslContextSupplier().get().isPresent());
        assertTrue("The keystore is still open", keystore.toFile().delete());
    }

    @Test
    public void keystoreDescriptorIsReleased() throws IOException {
        // On Linux the open file descriptors of the process are listed under `/proc/self/fd`
        Path fdDir = Paths.get("/proc/self/fd");
        assumeTrue("Not running on Linux", Files.isDirectory(fdDir));
        assertTrue(configuredSource().getSslContextSupplier().get().isPresent());
        String keystorePath = keystore.toRealPath().toString();
        try (Stream<Path> fds = Files.list(fdDir)) {
            List<Path> open =
                    fds.filter(fd -> keystorePath.equals(readLink(fd))).collect(Collectors.toList());
            assertTrue("File descriptors still open on the keystore: " + open, open.isEmpty());
        }
    }

    private static String readLink(Path fd) {
        try {
            return Files.readSymbolicLink(fd).toString();
        } catch (IOException e) {
            // The descriptor was closed between the listing and the read
            return null;
        }
    }
}
