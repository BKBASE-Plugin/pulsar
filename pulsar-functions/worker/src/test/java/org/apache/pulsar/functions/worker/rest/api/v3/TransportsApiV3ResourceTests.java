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

package org.apache.pulsar.functions.worker.rest.api.v3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.TransportConfigUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.rest.api.TransportsImpl;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.apache.pulsar.io.twitter.TwitterFireHose;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

/**
 * Unit test of {@link TransportsApiV3Resource}.
 */
@PrepareForTest({WorkerUtils.class, ConnectorUtils.class, FunctionCommon.class, ClassLoaderUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.*" })
public class TransportsApiV3ResourceTests {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String transportName = "test-source";
    private static final String sourceClassName = TwitterFireHose.class.getName();
    private static final String sinkClassName = CassandraStringSink.class.getName();
    private static final String functionClassName = IdentityFunction.class.getName();
    private static final int parallelism = 1;
    private static final String JAR_FILE_NAME = "pulsar-io-twitter.nar";
    private static final String INVALID_JAR_FILE_NAME = "pulsar-io-cassandra.nar";
    private String JAR_FILE_PATH;
    private String INVALID_JAR_FILE_PATH;

    private WorkerService mockedWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionMetaDataManager mockedManager;
    private FunctionRuntimeManager mockedFunctionRunTimeManager;
    private RuntimeFactory mockedRuntimeFactory;
    private Namespace mockedNamespace;
    private TransportsImpl transport;
    private InputStream mockedInputStream;
    private FormDataContentDisposition mockedFormData;
    private FunctionMetaData mockedFunctionMetaData;

    @BeforeMethod
    public void setup() throws Exception {
        this.mockedManager = mock(FunctionMetaDataManager.class);
        this.mockedFunctionRunTimeManager = mock(FunctionRuntimeManager.class);
        this.mockedRuntimeFactory = mock(RuntimeFactory.class);
        this.mockedInputStream = mock(InputStream.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedFormData = mock(FormDataContentDisposition.class);
        when(mockedFormData.getFileName()).thenReturn("test");
        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        namespaceList.add(tenant + "/" + namespace);

        this.mockedWorkerService = mock(WorkerService.class);
        when(mockedWorkerService.getFunctionMetaDataManager()).thenReturn(mockedManager);
        when(mockedWorkerService.getFunctionRuntimeManager()).thenReturn(mockedFunctionRunTimeManager);
        when(mockedFunctionRunTimeManager.getRuntimeFactory()).thenReturn(mockedRuntimeFactory);
        when(mockedWorkerService.getDlogNamespace()).thenReturn(mockedNamespace);
        when(mockedWorkerService.isInitialized()).thenReturn(true);
        when(mockedWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);
        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);

        URL file = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME);
        if (file == null)  {
            throw new RuntimeException("Failed to file required test archive: " + JAR_FILE_NAME);
        }
        JAR_FILE_PATH = file.getFile();
        INVALID_JAR_FILE_PATH = Thread.currentThread().getContextClassLoader().getResource(INVALID_JAR_FILE_NAME).getFile();

        // worker config
        WorkerConfig workerConfig = new WorkerConfig()
                .setWorkerId("test")
                .setWorkerPort(8080)
                .setDownloadDirectory("/tmp/pulsar/functions")
                .setFunctionMetadataTopicName("pulsar/functions")
                .setNumFunctionPackageReplicas(3)
                .setPulsarServiceUrl("pulsar://localhost:6650/");
        when(mockedWorkerService.getWorkerConfig()).thenReturn(workerConfig);

        this.transport = spy(new TransportsImpl(()-> mockedWorkerService));
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType(any())).thenReturn(ComponentType.TRANSPORT);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testRegisterTransportMissingTenant() {
        try {
            testRegisterTransportMissingArguments(
                    null,
                    namespace,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testRegisterTransportMissingNamespace() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    null,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport name is not provided")
    public void testRegisterTransportMissingTransportName() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    null,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Source class UnknownClass must be in class path")
    public void testRegisterTransportWrongClassName() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    "UnknownClass",
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport package is not provided")
    public void testRegisterTransportMissingPackage() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    null,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport Package is not provided")
    public void testRegisterTransportMissingPackageDetails() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    mockedInputStream,
                    null,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Failed to extract source class from archive")
    public void testRegisterTransportInvalidJarWithNoTransport() throws IOException {
        try {
            FileInputStream inputStream = new FileInputStream(INVALID_JAR_FILE_PATH);
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    inputStream,
                    null,
                    functionClassName,
                    sinkClassName,
                    null,
                    parallelism,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Encountered error .*. when getting Transport package from .*")
    public void testRegisterTransportHttpUrl() {
        try {
            testRegisterTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    null,
                    null,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    "http://localhost:1234/test"
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    private void testRegisterTransportMissingArguments(
            String tenant,
            String namespace,
            String transportName,
            InputStream inputStream,
            FormDataContentDisposition details,
            String functionClassName,
            String sinkClassName,
            String sourceClassName,
            Integer parallelism,
            String pkgUrl) {
        TransportConfig transportConfig = new TransportConfig();
        if (tenant != null) {
            transportConfig.setTenant(tenant);
        }
        if (namespace != null) {
            transportConfig.setNamespace(namespace);
        }
        if (transportName != null) {
            transportConfig.setName(transportName);
        }
        if (functionClassName != null) {
            transportConfig.setFunctionClassName(functionClassName);
        }
        if (sinkClassName != null) {
            transportConfig.setSinkClassName(sinkClassName);
        }
        if (sourceClassName != null) {
            transportConfig.setSourceClassName(sourceClassName);
        }
        if (parallelism != null) {
            transportConfig.setParallelism(parallelism);
        }

        transport.registerTransport(
                tenant,
                namespace,
                transportName,
                inputStream,
                details,
                pkgUrl,
                transportConfig,
                null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport config is not provided")
    public void testMissingTransportConfig() {
        transport.registerTransport(
                tenant,
                namespace,
                transportName,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport config is not provided")
    public void testUpdateMissingTransportConfig() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);
        transport.updateTransport(
                tenant,
                namespace,
                transportName,
                mockedInputStream,
                mockedFormData,
                null,
                null,
                null, null, null);
    }

    private void registerDefaultTransport() throws IOException {
        TransportConfig defaultTransportConfig = createDefaultTransportConfig();
        transport.registerTransport(
                tenant,
                namespace,
                transportName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                defaultTransportConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport test-source already exists")
    public void testRegisterExistedTransport() throws IOException {
        try {
            Configurator.setRootLevel(Level.DEBUG);

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testRegisterTransportUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);

            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testRegisterTransportSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("transport registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        registerDefaultTransport();
    }

    @Test
    public void testRegisterTransportConflictingFields() throws Exception {

        mockStatic(WorkerUtils.class);
        PowerMockito.doNothing().when(WorkerUtils.class, "uploadFileToBookkeeper", anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        String actualTenant = "DIFFERENT_TENANT";
        String actualNamespace = "DIFFERENT_NAMESPACE";
        String actualName = "DIFFERENT_NAME";
        this.namespaceList.add(actualTenant + "/" + actualNamespace);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);
        when(mockedManager.containsFunction(eq(actualTenant), eq(actualNamespace), eq(actualName))).thenReturn(false);

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("transport registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant(tenant);
        transportConfig.setNamespace(namespace);
        transportConfig.setName(transportName);
        transportConfig.setFunctionClassName(functionClassName);
        transportConfig.setParallelism(parallelism);
        transportConfig.setSinkClassName(sinkClassName);
        transportConfig.setSourceClassName(sourceClassName);
        transport.registerTransport(
                actualTenant,
                actualNamespace,
                actualName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                transportConfig,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "transport failed to register")
    public void testRegisterTransportFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("transport failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testRegisterTransportInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                    new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Update Functions
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testUpdateTransportMissingTenant() throws Exception {
        try {
            testUpdateTransportMissingArguments(
                    null,
                    namespace,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    "Tenant is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testUpdateTransportMissingNamespace() throws Exception {
        try {
            testUpdateTransportMissingArguments(
                    tenant,
                    null,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    "Namespace is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport name is not provided")
    public void testUpdateTransportMissingFunctionName() throws Exception {
        try {
            testUpdateTransportMissingArguments(
                    tenant,
                    namespace,
                    null,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism,
                    "Transport name is not provided");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Update contains no change")
    public void testUpdateTransportMissingPackage() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            testUpdateTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    null,
                    mockedFormData,
                    null,
                    null,
                    null,
                    parallelism,
                    "Update contains no change");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }


    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport parallelism must be a positive number")
    public void testUpdateTransportNegativeParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    null,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    -2,
                    "Source parallelism must be a positive number");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test
    public void testUpdateTransportChangedParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    null,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    parallelism + 1,
                    null);
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }



    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport parallelism must be a positive number")
    public void testUpdateTransportZeroParallelism() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.downloadFromBookkeeper(any(Namespace.class), any(File.class), anyString());

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            testUpdateTransportMissingArguments(
                    tenant,
                    namespace,
                    transportName,
                    mockedInputStream,
                    mockedFormData,
                    functionClassName,
                    sinkClassName,
                    sourceClassName,
                    0,
                    "Transport parallelism must be a positive number");
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testUpdateTransportMissingArguments(
            String tenant,
            String namespace,
            String function,
            InputStream inputStream,
            FormDataContentDisposition details,
            String functionClassName,
            String sinkClassName,
            String sourceClassName,
            Integer parallelism,
            String expectedError) throws Exception {

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));
        when(FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)).thenReturn(ProcessingGuarantees.ATLEAST_ONCE);
        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        TransportConfig transportConfig = new TransportConfig();
        if (tenant != null) {
            transportConfig.setTenant(tenant);
        }
        if (namespace != null) {
            transportConfig.setNamespace(namespace);
        }
        if (function != null) {
            transportConfig.setName(function);
        }
        if (functionClassName != null) {
            transportConfig.setFunctionClassName(functionClassName);
        }
        if (sinkClassName != null) {
            transportConfig.setSinkClassName(sinkClassName);
        }
        if (sourceClassName != null) {
            transportConfig.setSourceClassName(sourceClassName);
        }
        if (parallelism != null) {
            transportConfig.setParallelism(parallelism);
        }
        transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        if (expectedError == null) {
            RequestResult rr = new RequestResult()
                    .setSuccess(true)
                    .setMessage("transport registered");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);
        }

        transport.updateTransport(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                null,
                transportConfig,
                null, null, null);

    }

    private void updateDefaultSource() throws Exception {
        TransportConfig sourceConfig = new TransportConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(namespace);
        sourceConfig.setName(transportName);
        sourceConfig.setFunctionClassName(functionClassName);
        sourceConfig.setParallelism(parallelism);
        sourceConfig.setSinkClassName(sinkClassName);
        sourceConfig.setSourceClassName(sourceClassName);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        PowerMockito.when(FunctionCommon.class, "createPkgTempFile").thenCallRealMethod();
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));
        when(FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)).thenReturn(ProcessingGuarantees.ATLEAST_ONCE);
        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(File.class), any());

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);

        transport.updateTransport(
                tenant,
                namespace,
                transportName,
                new FileInputStream(JAR_FILE_PATH),
                mockedFormData,
                null,
                sourceConfig,
                null, null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport test-source doesn't exist")
    public void testUpdateNotExistedSource() throws Exception {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "upload failure")
    public void testUpdateTransportUploadFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doThrow(new IOException("upload failure")).when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);
            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    @Test
    public void testUpdateTransportSuccess() throws Exception {
        mockStatic(WorkerUtils.class);
        doNothing().when(WorkerUtils.class);
        WorkerUtils.uploadFileToBookkeeper(
                anyString(),
                any(File.class),
                any(Namespace.class));

        PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        updateDefaultSource();
    }

    @Test
    public void testUpdateTransportWithUrl() throws Exception {
        Configurator.setRootLevel(Level.DEBUG);

        String filePackageUrl = "file://" + JAR_FILE_PATH;

        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setSinkClassName(sinkClassName);
        transportConfig.setSourceClassName(sourceClassName);
        transportConfig.setTenant(tenant);
        transportConfig.setNamespace(namespace);
        transportConfig.setName(transportName);
        transportConfig.setFunctionClassName(functionClassName);
        transportConfig.setParallelism(parallelism);
        transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);

        mockStatic(ConnectorUtils.class);
        doReturn(TwitterFireHose.class.getName()).when(ConnectorUtils.class);
        ConnectorUtils.getIOSourceClass(any(NarClassLoader.class));

        mockStatic(ClassLoaderUtils.class);

        mockStatic(FunctionCommon.class);
        doReturn(String.class).when(FunctionCommon.class);
        FunctionCommon.getSourceType(anyString(), any(NarClassLoader.class));
        PowerMockito.when(FunctionCommon.class, "extractFileFromPkgURL", any()).thenCallRealMethod();

        doReturn(mock(NarClassLoader.class)).when(FunctionCommon.class);
        FunctionCommon.extractNarClassLoader(any(), any(), any());

        this.mockedFunctionMetaData = FunctionMetaData.newBuilder().setFunctionDetails(createDefaultFunctionDetails()).build();
        when(mockedManager.getFunctionMetaData(any(), any(), any())).thenReturn(mockedFunctionMetaData);
        when(FunctionCommon.convertProcessingGuarantee(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)).thenReturn(ProcessingGuarantees.ATLEAST_ONCE);

        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);
        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source registered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

        transport.updateTransport(
                tenant,
                namespace,
                transportName,
                null,
                null,
                filePackageUrl,
                transportConfig,
                null, null, null);

    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "transport failed to register")
    public void testUpdateTransportFailure() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("transport failed to register");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "java.io.IOException: Function registration interrupted")
    public void testUpdateTransportInterrupted() throws Exception {
        try {
            mockStatic(WorkerUtils.class);
            doNothing().when(WorkerUtils.class);
            WorkerUtils.uploadFileToBookkeeper(
                    anyString(),
                    any(File.class),
                    any(Namespace.class));

            PowerMockito.when(WorkerUtils.class, "dumpToTmpFile", any()).thenCallRealMethod();

            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                    new IOException("Function registration interrupted"));
            when(mockedManager.updateFunction(any(FunctionMetaData.class))).thenReturn(requestResult);

            updateDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // deregister source
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testDeregisterSourceMissingTenant() {
        try {
            testDeregisterSourceMissingArguments(
                    null,
                    namespace,
                    transportName
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testDeregisterSourceMissingNamespace() {
        try {
            testDeregisterSourceMissingArguments(
                    tenant,
                    null,
                    transportName
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport name is not provided")
    public void testDeregisterSourceMissingFunctionName() {
        try {
            testDeregisterSourceMissingArguments(
                    tenant,
                    namespace,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testDeregisterSourceMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        transport.deregisterFunction(
                tenant,
                namespace,
                function,
                null, null);

    }

    private void deregisterDefaultSource() {
        transport.deregisterFunction(
                tenant,
                namespace,
                transportName,
                null, null);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp= "Transport test-source doesn't exist")
    public void testDeregisterNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);
            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testDeregisterSourceSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(transportName))).thenReturn(FunctionMetaData.newBuilder().build());

        RequestResult rr = new RequestResult()
                .setSuccess(true)
                .setMessage("source deregistered");
        CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
        when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(requestResult);

        deregisterDefaultSource();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "source failed to deregister")
    public void testDeregisterSourceFailure() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(transportName))).thenReturn(FunctionMetaData.newBuilder().build());

            RequestResult rr = new RequestResult()
                    .setSuccess(false)
                    .setMessage("source failed to deregister");
            CompletableFuture<RequestResult> requestResult = CompletableFuture.completedFuture(rr);
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(requestResult);

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function deregistration interrupted")
    public void testDeregisterSourceInterrupted() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

            when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(transportName))).thenReturn(FunctionMetaData.newBuilder().build());

            CompletableFuture<RequestResult> requestResult = FutureUtil.failedFuture(
                    new IOException("Function deregistration interrupted"));
            when(mockedManager.deregisterFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(requestResult);

            deregisterDefaultSource();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.INTERNAL_SERVER_ERROR);
            throw re;
        }
    }

    //
    // Get Source Info
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testGetSourceMissingTenant() {
        try {
            testGetSourceMissingArguments(
                    null,
                    namespace,
                    transportName
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testGetSourceMissingNamespace() {
        try {
            testGetSourceMissingArguments(
                    tenant,
                    null,
                    transportName
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport name is not provided")
    public void testGetSourceMissingFunctionName() {
        try {
            testGetSourceMissingArguments(
                    tenant,
                    namespace,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testGetSourceMissingArguments(
            String tenant,
            String namespace,
            String source
    ) {
        transport.getFunctionInfo(
                tenant,
                namespace,
                source, null, null
        );
    }

    private TransportConfig getDefaultTransportInfo() {
        return transport.getTransportInfo(
                tenant,
                namespace,
                transportName
        );
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Transport test-source doesn't exist")
    public void testGetNotExistedSource() {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(false);
            getDefaultTransportInfo();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetTransportSuccess() {
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(transportName))).thenReturn(true);

        SourceSpec sourceSpec = SourceSpec.newBuilder().setBuiltin("jdbc").build();
        SinkSpec sinkSpec = SinkSpec.newBuilder().build();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
                .setClassName(IdentityFunction.class.getName())
                .setSink(sinkSpec)
                .setName(transportName)
                .setNamespace(namespace)
                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
                .setRuntime(FunctionDetails.Runtime.JAVA)
                .setAutoAck(true)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(sourceSpec).build();
        FunctionMetaData metaData = FunctionMetaData.newBuilder()
                .setCreateTime(System.currentTimeMillis())
                .setFunctionDetails(functionDetails)
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
                .setVersion(1234)
                .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(transportName))).thenReturn(metaData);

        TransportConfig config = getDefaultTransportInfo();
        assertEquals(TransportConfigUtils.convertFromDetails(functionDetails), config);
    }

    //
    // List Sources
    //

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant is not provided")
    public void testListSourcesMissingTenant() {
        try {
            testListSourcesMissingArguments(
                    null,
                    namespace
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace is not provided")
    public void testListSourcesMissingNamespace() {
        try {
            testListSourcesMissingArguments(
                    tenant,
                    null
            );
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private void testListSourcesMissingArguments(
            String tenant,
            String namespace
    ) {
        transport.listFunctions(
                tenant,
                namespace, null, null
        );
    }

    private List<String> listDefaultTransports() {
        return transport.listFunctions(
                tenant,
                namespace, null, null);
    }

    @Test
    public void testListSourcesSuccess() {
        final List<String> functions = Lists.newArrayList("test-1", "test-2");
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()
        ).build());
        functionMetaDataList.add(FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()
        ).build());
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);

        List<String> sourceList = listDefaultTransports();
        assertEquals(functions, sourceList);
    }

    @Test
    public void testOnlyGetTransports() {
        final List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        FunctionMetaData f1 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-1").build()).build();
        functionMetaDataList.add(f1);
        FunctionMetaData f2 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-2").build()).build();
        functionMetaDataList.add(f2);
        FunctionMetaData f3 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-3").build()).build();
        functionMetaDataList.add(f3);
        FunctionMetaData f4 = FunctionMetaData.newBuilder().setFunctionDetails(
                FunctionDetails.newBuilder().setName("test-4").build()).build();
        functionMetaDataList.add(f4);
        when(mockedManager.listFunctions(eq(tenant), eq(namespace))).thenReturn(functionMetaDataList);
        mockStatic(InstanceUtils.class);
        PowerMockito.when(InstanceUtils.calculateSubjectType((f1.getFunctionDetails()))).thenReturn(Function.FunctionDetails.ComponentType.SOURCE);
        PowerMockito.when(InstanceUtils.calculateSubjectType((f2.getFunctionDetails()))).thenReturn(Function.FunctionDetails.ComponentType.FUNCTION);
        PowerMockito.when(InstanceUtils.calculateSubjectType((f3.getFunctionDetails()))).thenReturn(Function.FunctionDetails.ComponentType.SINK);
        PowerMockito.when(InstanceUtils.calculateSubjectType((f4.getFunctionDetails()))).thenReturn(Function.FunctionDetails.ComponentType.TRANSPORT);

        List<String> transportList = listDefaultTransports();
        final List<String> functions = Lists.newArrayList("test-4");
        assertEquals(functions, transportList);
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Namespace does not exist")
    public void testRegisterFunctionNonExistingNamespace() throws Exception {
        try {
            this.namespaceList.clear();
            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Tenant does not exist")
    public void testRegisterFunctionNonExistingTenant() throws Exception {
        try {
            when(mockedTenants.getTenantInfo(any())).thenThrow(PulsarAdminException.NotFoundException.class);
            registerDefaultTransport();
        } catch (RestException re){
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.BAD_REQUEST);
            throw re;
        }
    }

    private TransportConfig createDefaultTransportConfig() {
        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant(tenant);
        transportConfig.setNamespace(namespace);
        transportConfig.setName(transportName);
        transportConfig.setFunctionClassName(functionClassName);
        transportConfig.setSinkClassName(sinkClassName);
        transportConfig.setSourceClassName(sourceClassName);
        transportConfig.setParallelism(parallelism);
        return transportConfig;
    }

    private FunctionDetails createDefaultFunctionDetails() {
        return TransportConfigUtils.convert(createDefaultTransportConfig(),
                new TransportConfigUtils.ExtractedTransportDetails(null, null, null, null));
    }

}
