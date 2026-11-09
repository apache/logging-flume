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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.flume.FlumeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Resolves the {@code type} configuration property of one component kind to a component class.
 *
 * <p>A type is either the name of a provider registered through {@link ServiceLoader},
 * compared case-insensitively,
 * or the fully qualified name of the component class.
 */
public final class ComponentRegistry<T> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final Class<?> providerType;
    private final Function<Object, String> nameOf;
    private final Function<Object, ? extends Class<? extends T>> componentClassOf;
    private final Class<T> componentType;
    private final String kind;
    private final Function<String, String> legacyAliases;

    private volatile Providers<T> providers;

    /**
     * @param providerType the service interface of this component kind, such as {@link SourceProvider}.
     * @param componentType the class all components of this kind extend.
     * @param kind a human-readable name of the component kind, used in error messages.
     */
    public ComponentRegistry(Class<? extends ComponentProvider<T>> providerType, Class<T> componentType, String kind) {
        this(providerType, componentType, kind, type -> null);
    }

    /**
     * Creates a registry that also accepts the aliases of a legacy type enum.
     *
     * @param legacyAliases maps a type to a class name, or {@code null} if the type is not a legacy alias.
     * @deprecated Legacy aliases will be removed together with the type enums.
     */
    @Deprecated
    public ComponentRegistry(
            Class<? extends ComponentProvider<T>> providerType,
            Class<T> componentType,
            String kind,
            Function<String, String> legacyAliases) {
        this(
                providerType,
                provider -> provider.getName(),
                provider -> provider.getComponentClass(),
                componentType,
                kind,
                legacyAliases);
    }

    @SuppressWarnings("unchecked")
    private <P> ComponentRegistry(
            Class<P> providerType,
            Function<? super P, String> nameOf,
            Function<? super P, ? extends Class<? extends T>> componentClassOf,
            Class<T> componentType,
            String kind,
            Function<String, String> legacyAliases) {
        this.providerType = Objects.requireNonNull(providerType, "providerType");
        // Only applied to instances of `providerType`
        this.nameOf = (Function<Object, String>) Objects.requireNonNull(nameOf, "nameOf");
        this.componentClassOf = (Function<Object, ? extends Class<? extends T>>)
                Objects.requireNonNull(componentClassOf, "componentClassOf");
        this.componentType = Objects.requireNonNull(componentType, "componentType");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.legacyAliases = Objects.requireNonNull(legacyAliases, "legacyAliases");
    }

    /**
     * Creates a registry for a provider interface that does not extend {@link ComponentProvider}.
     *
     * <p>Used by component kinds whose API module does not depend on {@code flume-core},
     * such as {@code org.apache.flume.configfilter.ConfigFilterProvider}.
     *
     * @param providerType the service interface of this component kind.
     * @param nameOf returns the name of a provider.
     * @param componentClassOf returns the component class of a provider.
     * @param componentType the class all components of this kind extend.
     * @param kind a human-readable name of the component kind, used in error messages.
     */
    public static <P, T> ComponentRegistry<T> of(
            Class<P> providerType,
            Function<? super P, String> nameOf,
            Function<? super P, ? extends Class<? extends T>> componentClassOf,
            Class<T> componentType,
            String kind) {
        return new ComponentRegistry<>(providerType, nameOf, componentClassOf, componentType, kind, type -> null);
    }

    /**
     * Creates a registry for a provider interface that does not extend {@link ComponentProvider},
     * which also accepts the aliases of a legacy type enum.
     *
     * @param legacyAliases maps a type to a class name, or {@code null} if the type is not a legacy alias.
     * @deprecated Legacy aliases will be removed together with the type enums.
     * @see #of(Class, Function, Function, Class, String)
     */
    @Deprecated
    public static <P, T> ComponentRegistry<T> of(
            Class<P> providerType,
            Function<? super P, String> nameOf,
            Function<? super P, ? extends Class<? extends T>> componentClassOf,
            Class<T> componentType,
            String kind,
            Function<String, String> legacyAliases) {
        return new ComponentRegistry<>(providerType, nameOf, componentClassOf, componentType, kind, legacyAliases);
    }

    /**
     * Returns the component class selected by a type.
     *
     * @throws FlumeException if the type is unknown or does not select a component of this kind.
     */
    public Class<? extends T> getComponentClass(String type) throws FlumeException {
        Objects.requireNonNull(type, "type");
        Providers<T> current = getProviders();
        String name = type.toLowerCase(Locale.ROOT);
        String conflict = current.conflicts.get(name);
        if (conflict != null) {
            throw new FlumeException(conflict);
        }
        Class<? extends T> registered = current.classes.get(name);
        if (registered != null) {
            return registered;
        }
        String legacyClassName = legacyAliases.apply(type);
        String className = legacyClassName != null ? legacyClassName : type;
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, ComponentRegistry.class.getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            throw new FlumeException(
                    "Unknown " + kind + " type " + type + ": add the artifact that provides it, or use one of "
                            + current.classes.keySet(),
                    e);
        }
        if (!componentType.isAssignableFrom(clazz)) {
            throw new FlumeException("The " + kind + " type " + type + " selects " + clazz.getName()
                    + ", which does not implement " + componentType.getName());
        }
        return clazz.asSubclass(componentType);
    }

    /**
     * Creates a new component of the given type, with its no-argument constructor.
     *
     * @throws FlumeException if the type is unknown or the component cannot be instantiated.
     */
    public T newInstance(String type) throws FlumeException {
        Class<? extends T> clazz = getComponentClass(type);
        try {
            return clazz.getConstructor().newInstance();
        } catch (ReflectiveOperationException | RuntimeException e) {
            throw new FlumeException("Unable to create " + kind + " of type " + type + ", class " + clazz.getName(), e);
        }
    }

    private Providers<T> getProviders() {
        Providers<T> current = providers;
        if (current == null) {
            current = loadProviders();
            providers = current;
        }
        return current;
    }

    private Providers<T> loadProviders() {
        Map<String, Class<? extends T>> classes = new TreeMap<>();
        Map<String, Object> owners = new HashMap<>();
        Map<String, String> conflicts = new HashMap<>();
        Iterator<?> iterator = ServiceLoader.load(providerType, ComponentRegistry.class.getClassLoader())
                .iterator();
        while (true) {
            Object provider;
            String name;
            Class<? extends T> clazz;
            try {
                if (!iterator.hasNext()) {
                    break;
                }
                provider = iterator.next();
                name = nameOf.apply(provider).toLowerCase(Locale.ROOT);
                clazz = componentClassOf.apply(provider);
            } catch (ServiceConfigurationError | LinkageError e) {
                // A broken provider must not prevent the other ones from loading
                LOGGER.warn("Skipping a {} provider that failed to load.", kind, e);
                continue;
            }
            Object previous = owners.putIfAbsent(name, provider);
            if (previous == null) {
                classes.put(name, clazz);
            } else {
                // Fail only when the ambiguous type is used
                LOGGER.error(
                        "The {} type {} is provided by both {} and {}.",
                        kind,
                        name,
                        previous.getClass().getName(),
                        provider.getClass().getName());
                conflicts.put(
                        name,
                        "The " + kind + " type " + name + " is provided by both "
                                + previous.getClass().getName() + " and "
                                + provider.getClass().getName());
            }
        }
        return new Providers<>(Collections.unmodifiableMap(classes), Collections.unmodifiableMap(conflicts));
    }

    private static final class Providers<T> {

        private final Map<String, Class<? extends T>> classes;
        private final Map<String, String> conflicts;

        private Providers(Map<String, Class<? extends T>> classes, Map<String, String> conflicts) {
            this.classes = classes;
            this.conflicts = conflicts;
        }
    }
}
