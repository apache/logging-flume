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
package org.apache.flume.channel.file;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Releases {@link MappedByteBuffer}s eagerly instead of waiting for garbage collection.
 * On Windows a memory-mapped file cannot be deleted while a mapping is alive, so the
 * channel must unmap its buffers before checkpoint files can be removed or replaced.
 */
final class MappedFiles {
    private static final Logger logger = LogManager.getLogger();
    private static final Object UNSAFE;
    private static final Method INVOKE_CLEANER;

    static {
        // `jdk.unsupported` exports and opens `sun.misc`, so no JVM flags are needed.
        Object unsafe = null;
        Method invokeCleaner = null;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = theUnsafe.get(null);
            invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (ReflectiveOperationException | RuntimeException e) {
            logger.warn(
                    "Cannot unmap memory-mapped files explicitly. Deleting checkpoint"
                            + " files may fail until the mappings are garbage collected.",
                    e);
        }
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    private MappedFiles() {}

    /**
     * Unmaps the given buffer. The buffer must never be accessed afterwards:
     * reading or writing an unmapped buffer crashes the JVM.
     *
     * @param buffer the buffer returned by {@code FileChannel.map}, may be {@code null}
     */
    static void unmap(MappedByteBuffer buffer) {
        if (buffer == null || INVOKE_CLEANER == null) {
            return;
        }
        try {
            INVOKE_CLEANER.invoke(UNSAFE, buffer);
        } catch (ReflectiveOperationException | RuntimeException e) {
            logger.warn("Failed to unmap buffer", e);
        }
    }
}
