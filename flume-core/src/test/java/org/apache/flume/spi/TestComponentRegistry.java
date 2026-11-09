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
package org.apache.flume.spi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import org.apache.flume.FlumeException;
import org.junit.Test;

public class TestComponentRegistry {

    /** Test-only component kind, registered in {@code META-INF/services}. */
    public interface FakeProvider extends ComponentProvider<Runnable> {}

    public static final class FakeComponent implements Runnable {
        @Override
        public void run() {}
    }

    public static final class OtherFakeComponent implements Runnable {
        @Override
        public void run() {}
    }

    public static final class UnregisteredComponent implements Runnable {
        @Override
        public void run() {}
    }

    public static final class FakeProviderOne extends AbstractComponentProvider<Runnable> implements FakeProvider {
        public FakeProviderOne() {
            super("One", FakeComponent.class);
        }
    }

    public static final class DuplicateProviderA extends AbstractComponentProvider<Runnable> implements FakeProvider {
        public DuplicateProviderA() {
            super("duplicate", FakeComponent.class);
        }
    }

    public static final class DuplicateProviderB extends AbstractComponentProvider<Runnable> implements FakeProvider {
        public DuplicateProviderB() {
            super("duplicate", OtherFakeComponent.class);
        }
    }

    private final ComponentRegistry<Runnable> registry =
            new ComponentRegistry<>(FakeProvider.class, Runnable.class, "fake component");

    @Test
    public void resolvesRegisteredNameIgnoringCase() {
        assertEquals(FakeComponent.class, registry.getComponentClass("one"));
        assertEquals(FakeComponent.class, registry.getComponentClass("ONE"));
    }

    @Test
    public void resolvesUnregisteredClassName() {
        assertEquals(UnregisteredComponent.class, registry.getComponentClass(UnregisteredComponent.class.getName()));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void resolvesLegacyAlias() {
        ComponentRegistry<Runnable> legacy = new ComponentRegistry<>(
                FakeProvider.class,
                Runnable.class,
                "fake component",
                type -> "legacy".equalsIgnoreCase(type) ? UnregisteredComponent.class.getName() : null);
        assertEquals(UnregisteredComponent.class, legacy.getComponentClass("Legacy"));
        // Registered names take precedence over legacy aliases
        assertEquals(FakeComponent.class, legacy.getComponentClass("one"));
    }

    @Test
    public void newInstanceCreatesDistinctInstances() {
        Runnable first = registry.newInstance("one");
        Runnable second = registry.newInstance("one");
        assertTrue(first instanceof FakeComponent);
        assertTrue(first != second);
    }

    @Test
    public void rejectsUnknownType() {
        FlumeException e = assertThrows(FlumeException.class, () -> registry.getComponentClass("unknown"));
        assertTrue(e.getMessage(), e.getMessage().contains("[duplicate, one]"));
    }

    @Test
    public void rejectsClassOfWrongKind() {
        FlumeException e =
                assertThrows(FlumeException.class, () -> registry.getComponentClass(Callable.class.getName()));
        assertTrue(e.getMessage(), e.getMessage().contains(Runnable.class.getName()));
    }

    @Test
    public void rejectsAmbiguousName() {
        FlumeException e = assertThrows(FlumeException.class, () -> registry.getComponentClass("duplicate"));
        assertTrue(e.getMessage(), e.getMessage().contains(DuplicateProviderA.class.getName()));
        assertTrue(e.getMessage(), e.getMessage().contains(DuplicateProviderB.class.getName()));
    }
}
