/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.utils;

import static org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.gson.Gson;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.common.validator.ConfigValidationAnnotations;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit test of {@link Reflections}.
 */
@PrepareForTest(ConnectorUtils.class)
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.springframework.context.*", "org.apache.log4j.*", "com.sun.org.apache.xerces.*", "javax.management.*" })
public class TransportConfigUtilsTest extends PowerMockTestCase {

    private ConnectorDefinition defn;

    @Data
    @Accessors(chain = true)
    @NoArgsConstructor
    public static class TestTransportConfig {
        @ConfigValidationAnnotations.NotNull
        private String configParameter;
    }

    @Test
    public void testConvertBackFidelity() throws IOException  {
        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant("test-tenant");
        transportConfig.setNamespace("test-namespace");
        transportConfig.setName("test-source");
        transportConfig.setParallelism(1);
        transportConfig.setArchive("builtin://jdbc");
        Map<String, ConsumerConfig> inputSpecs = new HashMap<>();
        inputSpecs.put("test-input", ConsumerConfig.builder().isRegexPattern(true).receiverQueueSize(532).serdeClassName("test-serde").build());
        transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        Map<String, String> producerConfigs = new HashMap<>();
        producerConfigs.put("security.protocal", "SASL_PLAINTEXT");
        Map<String, Object> configs = new HashMap<>();
        configs.put("topic", "kafka");
        configs.put("bootstrapServers", "server-1,server-2");
        configs.put("producerConfigProperties", producerConfigs);

        transportConfig.setConfigs(configs);
        transportConfig.setRetainOrdering(false);
        transportConfig.setAutoAck(true);
        transportConfig.setRuntimeFlags("-DKerberos");
        transportConfig.setFunctionClassName(IdentityFunction.class.getName());
        transportConfig.setUserConfig(new HashMap<>());
        Function.FunctionDetails functionDetails = TransportConfigUtils.convert(transportConfig, new TransportConfigUtils.ExtractedTransportDetails(null, null, null, null));
        TransportConfig convertedConfig = TransportConfigUtils.convertFromDetails(functionDetails);
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(convertedConfig)
        );
    }

    @Test
    public void testMergeEqual() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createTransportConfig();
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Transport Names differ")
    public void testMergeDifferentName() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("name", "Different");
        TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Tenants differ")
    public void testMergeDifferentTenant() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("tenant", "Different");
        TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Namespaces differ")
    public void testMergeDifferentNamespace() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("namespace", "Different");
        TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }

    @Test
    public void testMergeDifferentClassName() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("functionClassName", "Different");
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                mergedConfig.getFunctionClassName(),
                "Different"
        );
        mergedConfig.setFunctionClassName(transportConfig.getFunctionClassName());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }



    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Processing Guarantees cannot be altered")
    public void testMergeDifferentProcessingGuarantees() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("processingGuarantees", EFFECTIVELY_ONCE);
        TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Retain Ordering cannot be altered")
    public void testMergeDifferentRetainOrdering() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("retainOrdering", true);
        TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }

    @Test
    public void testMergeDifferentUserConfig() {
        TransportConfig transportConfig = createTransportConfig();
        Map<String, String> myConfig = new HashMap<>();
        myConfig.put("MyKey", "MyValue");
        TransportConfig newTransportConfig = createUpdatedTransportConfig("configs", myConfig);
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                mergedConfig.getConfigs(),
                myConfig
        );
        mergedConfig.setConfigs(transportConfig.getConfigs());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentSecrets() {
        TransportConfig transportConfig = createTransportConfig();
        Map<String, String> mySecrets = new HashMap<>();
        mySecrets.put("MyKey", "MyValue");
        TransportConfig newTransportConfig = createUpdatedTransportConfig("secrets", mySecrets);
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                mergedConfig.getSecrets(),
                mySecrets
        );
        mergedConfig.setSecrets(transportConfig.getSecrets());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "AutoAck cannot be altered")
    public void testMergeDifferentAutoAck() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("autoAck", false);
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
    }


    @Test
    public void testMergeDifferentParallelism() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newTransportConfig = createUpdatedTransportConfig("parallelism", 101);
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                mergedConfig.getParallelism(),
                new Integer(101)
        );
        mergedConfig.setParallelism(transportConfig.getParallelism());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }

    @Test
    public void testMergeDifferentResources() {
        TransportConfig transportConfig = createTransportConfig();
        Resources resources = new Resources();
        resources.setCpu(0.3);
        resources.setRam(1232l);
        resources.setDisk(123456l);
        TransportConfig newTransportConfig = createUpdatedTransportConfig("resources", resources);
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newTransportConfig);
        assertEquals(
                mergedConfig.getResources(),
                resources
        );
        mergedConfig.setResources(transportConfig.getResources());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }


    @Test
    public void testMergeRuntimeFlags() {
        TransportConfig transportConfig = createTransportConfig();
        TransportConfig newFunctionConfig = createUpdatedTransportConfig("runtimeFlags", "-Dfoo=bar2");
        TransportConfig mergedConfig = TransportConfigUtils.validateUpdate(transportConfig, newFunctionConfig);
        assertEquals(
                mergedConfig.getRuntimeFlags(), "-Dfoo=bar2"
        );
        mergedConfig.setRuntimeFlags(transportConfig.getRuntimeFlags());
        assertEquals(
                new Gson().toJson(transportConfig),
                new Gson().toJson(mergedConfig)
        );
    }


    private TransportConfig createTransportConfig() {
        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant("test-tenant");
        transportConfig.setNamespace("test-namespace");
        transportConfig.setName("test-transport");
        transportConfig.setParallelism(1);
        transportConfig.setFunctionClassName(IdentityFunction.class.getName());
        transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        transportConfig.setRetainOrdering(false);
        transportConfig.setConfigs(new HashMap<>());
        transportConfig.setAutoAck(true);
        transportConfig.setArchive("DummyArchive.nar");
        return transportConfig;
    }

    private TransportConfig createUpdatedTransportConfig(String fieldName, Object fieldValue) {
        TransportConfig transportConfig = createTransportConfig();
        Class<?> fClass = TransportConfig.class;
        try {
            Field chap = fClass.getDeclaredField(fieldName);
            chap.setAccessible(true);
            chap.set(transportConfig, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("Something wrong with the test", e);
        }
        return transportConfig;
    }
}
