package org.apache.pulsar.admin.cli;

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.beust.jcommander.ParameterException;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.Transports;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

@Slf4j
@PrepareForTest({CmdFunctions.class})
@PowerMockIgnore({"javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*", "org.apache.pulsar.io.core.*", "org.apache.pulsar.functions.api.*"})
public class TestCmdTransports {

    private static final String JAR_FILE_NAME = "dummy.nar";
    private static final String TENANT = "test-tenant";
    private static final String NAMESPACE = "test-namespace";
    private static final String NAME = "test";
    private static final FunctionConfig.ProcessingGuarantees PROCESSING_GUARANTEES
            = FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE;
    private static final Integer PARALLELISM = 1;
    private static final Double CPU = 100.0;
    private static final Long RAM = 1024L * 1024L;
    private static final Long DISK = 1024L * 1024L * 1024L;
    private static final String TRANSPORT_CONFIG_STRING = "{\"created_at\":\"Mon Jul 02 00:33:15 0000 2018\"}";
    private String JAR_FILE_PATH;

    private PulsarAdmin pulsarAdmin;
    private Transports transports;
    private CmdTransports cmdTransports;
    private CmdTransports.CreateTransport createTransport;
    private CmdTransports.UpdateTransport updateTransport;
    private CmdTransports.LocalTransportRunner localTransportRunner;
    private CmdTransports.DeleteTransport deleteTransport;

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @BeforeMethod
    public void setup() throws Exception {
        pulsarAdmin = mock(PulsarAdmin.class);
        transports = mock(Transports.class);

        when(pulsarAdmin.transports()).thenReturn(transports);
        cmdTransports = spy(new CmdTransports(pulsarAdmin));
        createTransport = spy(cmdTransports.getCreateTransport());
        updateTransport = spy(cmdTransports.getUpdateTransport());
        localTransportRunner = spy(cmdTransports.getLocalTransportRunner());
        deleteTransport = spy(cmdTransports.getDeleteTransport());

        mockStatic(CmdFunctions.class);
        PowerMockito.doNothing().when(localTransportRunner).runCmd();

        URL file = Thread.currentThread().getContextClassLoader().getResource(JAR_FILE_NAME);
        if (file == null) {
            throw new RuntimeException("Failed to file required test archive: " + JAR_FILE_NAME);
        }
        JAR_FILE_PATH = file.getFile();
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.loadJar(new File(JAR_FILE_PATH)));
    }

    public TransportConfig getTransportConfig() {
        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant(TENANT);
        transportConfig.setNamespace(NAMESPACE);
        transportConfig.setName(NAME);

        transportConfig.setProcessingGuarantees(PROCESSING_GUARANTEES);
        transportConfig.setParallelism(PARALLELISM);
        transportConfig.setArchive(JAR_FILE_PATH);
        transportConfig.setResources(new Resources(CPU, RAM, DISK));
        transportConfig.setConfigs(createTransport.parseConfigs(TRANSPORT_CONFIG_STRING));
        transportConfig.setUserConfig(new HashMap<>());
        return transportConfig;
    }

    public void testCmdTransportCliMissingArgs(
            String tenant,
            String namespace,
            String name,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String transportConfigString,
            TransportConfig transportConfig) throws Exception {

        // test create sink
        createTransport.tenant = tenant;
        createTransport.namespace = namespace;
        createTransport.name = name;
        createTransport.processingGuarantees = processingGuarantees;
        createTransport.parallelism = parallelism;
        createTransport.archive = jarFile;
        createTransport.cpu = cpu;
        createTransport.ram = ram;
        createTransport.disk = disk;
        createTransport.transportConfigString = transportConfigString;

        createTransport.processArguments();

        createTransport.runCmd();

        // test update sink
        updateTransport.tenant = tenant;
        updateTransport.namespace = namespace;
        updateTransport.name = name;
        updateTransport.processingGuarantees = processingGuarantees;
        updateTransport.parallelism = parallelism;
        updateTransport.archive = jarFile;
        updateTransport.cpu = cpu;
        updateTransport.ram = ram;
        updateTransport.disk = disk;
        updateTransport.transportConfigString = transportConfigString;

        updateTransport.processArguments();

        updateTransport.runCmd();

        // test local runner
        localTransportRunner.tenant = tenant;
        localTransportRunner.namespace = namespace;
        localTransportRunner.name = name;
        localTransportRunner.processingGuarantees = processingGuarantees;
        localTransportRunner.parallelism = parallelism;
        localTransportRunner.archive = jarFile;
        localTransportRunner.cpu = cpu;
        localTransportRunner.ram = ram;
        localTransportRunner.disk = disk;
        localTransportRunner.transportConfigString = transportConfigString;

        localTransportRunner.processArguments();

        localTransportRunner.runCmd();

        verify(createTransport).validateTransportConfigs(eq(transportConfig));
        verify(updateTransport).validateTransportConfigs(eq(transportConfig));
        verify(localTransportRunner).validateTransportConfigs(eq(transportConfig));
    }


    @Test
    public void testCliCorrect() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        testCmdTransportCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                TRANSPORT_CONFIG_STRING,
                transportConfig
        );
    }

    @Test
    public void testMissingProcessingGuarantees() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        transportConfig.setProcessingGuarantees(null);
        testCmdTransportCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                null,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                TRANSPORT_CONFIG_STRING,
                transportConfig
        );
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Transport archive not specfied")
    public void testMissingArchive() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        transportConfig.setArchive(null);
        testCmdTransportCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                null,
                CPU,
                RAM,
                DISK,
                TRANSPORT_CONFIG_STRING,
                transportConfig
        );
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Transport Archive file /tmp/foo.jar" +
            " does not exist")
    public void testInvalidJar() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        String fakeJar = "/tmp/foo.jar";
        transportConfig.setArchive(fakeJar);
        testCmdTransportCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                fakeJar,
                CPU,
                RAM,
                DISK,
                TRANSPORT_CONFIG_STRING,
                transportConfig
        );
    }

    @Test
    public void testMissingConfig() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        transportConfig.setConfigs(null);
        testCmdTransportCliMissingArgs(
                TENANT,
                NAMESPACE,
                NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                null,
                transportConfig
        );
    }

    public void testCmdTransportConfigFile(TransportConfig testTransportConfig, TransportConfig expectedTransportConfig) throws Exception {

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testTransportConfig);

        Assert.assertEquals(testTransportConfig, CmdUtils.loadConfig(file.getAbsolutePath(), TransportConfig.class));

        // test create sink
        createTransport.transportConfigFile = file.getAbsolutePath();

        createTransport.processArguments();
        createTransport.runCmd();

        // test update sink
        updateTransport.transportConfigFile = file.getAbsolutePath();

        updateTransport.processArguments();
        updateTransport.runCmd();

        // test local runner
        localTransportRunner.transportConfigFile = file.getAbsolutePath();

        localTransportRunner.processArguments();
        localTransportRunner.runCmd();

        verify(createTransport).validateTransportConfigs(eq(expectedTransportConfig));
        verify(updateTransport).validateTransportConfigs(eq(expectedTransportConfig));
        verify(localTransportRunner).validateTransportConfigs(eq(expectedTransportConfig));
    }


    @Test
    public void testCmdTransportConfigFileCorrect() throws Exception {
        TransportConfig transportConfig = getTransportConfig();
        testCmdTransportConfigFile(transportConfig, transportConfig);
    }

    @Test
    public void testCmdTransportConfigFileMissingConfig() throws Exception {
        TransportConfig testTransportConfig = getTransportConfig();
        testTransportConfig.setConfigs(null);

        TransportConfig expectedTransportConfig = getTransportConfig();
        expectedTransportConfig.setConfigs(null);
        testCmdTransportConfigFile(testTransportConfig, expectedTransportConfig);
    }

    @Test
    public void testCmdTransportConfigFileMissingProcessingGuarantees() throws Exception {
        TransportConfig testTransportConfig = getTransportConfig();
        testTransportConfig.setProcessingGuarantees(null);

        TransportConfig expectedTransportConfig = getTransportConfig();
        expectedTransportConfig.setProcessingGuarantees(null);
        testCmdTransportConfigFile(testTransportConfig, expectedTransportConfig);
    }

    @Test
    public void testCmdTransportConfigFileMissingResources() throws Exception {
        TransportConfig testTransportConfig = getTransportConfig();
        testTransportConfig.setResources(null);

        TransportConfig expectedTransportConfig = getTransportConfig();
        expectedTransportConfig.setResources(null);
        testCmdTransportConfigFile(testTransportConfig, expectedTransportConfig);
    }

    @Test(expectedExceptions = ParameterException.class, expectedExceptionsMessageRegExp = "Transport archive not specfied")
    public void testCmdTransportConfigFileMissingJar() throws Exception {
        TransportConfig testTransportConfig = getTransportConfig();
        testTransportConfig.setArchive(null);

        TransportConfig expectedTransportConfig = getTransportConfig();
        expectedTransportConfig.setArchive(null);
        testCmdTransportConfigFile(testTransportConfig, expectedTransportConfig);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Transport Archive file /tmp/foo.jar does not exist")
    public void testCmdTransportConfigFileInvalidJar() throws Exception {
        TransportConfig testTransportConfig = getTransportConfig();
        testTransportConfig.setArchive("/tmp/foo.jar");

        TransportConfig expectedTransportConfig = getTransportConfig();
        expectedTransportConfig.setArchive("/tmp/foo.jar");
        testCmdTransportConfigFile(testTransportConfig, expectedTransportConfig);
    }

    @Test
    public void testCliOverwriteConfigFile() throws Exception {

        TransportConfig testTransportConfig = new TransportConfig();
        testTransportConfig.setTenant(TENANT + "-prime");
        testTransportConfig.setNamespace(NAMESPACE + "-prime");
        testTransportConfig.setName(NAME + "-prime");


        testTransportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        testTransportConfig.setParallelism(PARALLELISM + 1);
        testTransportConfig.setArchive(JAR_FILE_PATH + "-prime");
        testTransportConfig.setResources(new Resources(CPU + 1, RAM + 1, DISK + 1));
        testTransportConfig.setConfigs(createTransport.parseConfigs("{\"created_at-prime\":\"Mon Jul 02 00:33:15 +0000 2018\", \"otherConfigProperties\":{\"property1.value\":\"value1\",\"property2.value\":\"value2\"}}"));


        TransportConfig expectedTransportConfig= getTransportConfig();

        File file = Files.createTempFile("", "").toFile();

        new YAMLMapper().writeValue(file, testTransportConfig);

        Assert.assertEquals(testTransportConfig, CmdUtils.loadConfig(file.getAbsolutePath(), TransportConfig.class));

        testMixCliAndConfigFile(
                TENANT,
                NAMESPACE,
                NAME,
                PROCESSING_GUARANTEES,
                PARALLELISM,
                JAR_FILE_PATH,
                CPU,
                RAM,
                DISK,
                TRANSPORT_CONFIG_STRING,
                file.getAbsolutePath(),
                expectedTransportConfig
        );
    }

    public void testMixCliAndConfigFile(
            String tenant,
            String namespace,
            String name,
            FunctionConfig.ProcessingGuarantees processingGuarantees,
            Integer parallelism,
            String jarFile,
            Double cpu,
            Long ram,
            Long disk,
            String transportConfigString,
            String transportConfigFile,
            TransportConfig transportConfig
    ) throws Exception {


        // test create sink
        createTransport.tenant = tenant;
        createTransport.namespace = namespace;
        createTransport.name = name;
        createTransport.processingGuarantees = processingGuarantees;
        createTransport.parallelism = parallelism;
        createTransport.archive = jarFile;
        createTransport.cpu = cpu;
        createTransport.ram = ram;
        createTransport.disk = disk;
        createTransport.transportConfigString = transportConfigString;
        createTransport.transportConfigFile = transportConfigFile;

        createTransport.processArguments();

        createTransport.runCmd();

        // test update sink
        updateTransport.tenant = tenant;
        updateTransport.namespace = namespace;
        updateTransport.name = name;
        updateTransport.processingGuarantees = processingGuarantees;
        updateTransport.parallelism = parallelism;
        updateTransport.archive = jarFile;
        updateTransport.cpu = cpu;
        updateTransport.ram = ram;
        updateTransport.disk = disk;
        updateTransport.transportConfigString = transportConfigString;
        updateTransport.transportConfigFile = transportConfigFile;

        updateTransport.processArguments();

        updateTransport.runCmd();

        // test local runner
        localTransportRunner.tenant = tenant;
        localTransportRunner.namespace = namespace;
        localTransportRunner.name = name;
        localTransportRunner.processingGuarantees = processingGuarantees;
        localTransportRunner.parallelism = parallelism;
        localTransportRunner.archive = jarFile;
        localTransportRunner.cpu = cpu;
        localTransportRunner.ram = ram;
        localTransportRunner.disk = disk;
        localTransportRunner.transportConfigString = transportConfigString;
        localTransportRunner.transportConfigFile = transportConfigFile;


        localTransportRunner.processArguments();

        localTransportRunner.runCmd();

        verify(createTransport).validateTransportConfigs(eq(transportConfig));
        verify(updateTransport).validateTransportConfigs(eq(transportConfig));
        verify(localTransportRunner).validateTransportConfigs(eq(transportConfig));
    }

    @Test
    public void testDeleteMissingTenant() throws Exception {
        deleteTransport.tenant = null;
        deleteTransport.namespace = NAMESPACE;
        deleteTransport.transportName = NAME;

        deleteTransport.processArguments();

        deleteTransport.runCmd();

        verify(transports).deleteTransport(eq(PUBLIC_TENANT), eq(NAMESPACE), eq(NAME));
    }

    @Test
    public void testDeleteMissingNamespace() throws Exception {
        deleteTransport.tenant = TENANT;
        deleteTransport.namespace = null;
        deleteTransport.transportName = NAME;

        deleteTransport.processArguments();

        deleteTransport.runCmd();

        verify(transports).deleteTransport(eq(TENANT), eq(DEFAULT_NAMESPACE), eq(NAME));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "You must specify a name for the transport")
    public void testDeleteMissingName() throws Exception {
        deleteTransport.tenant = TENANT;
        deleteTransport.namespace = NAMESPACE;
        deleteTransport.transportName = null;

        deleteTransport.processArguments();

        deleteTransport.runCmd();

        verify(transports).deleteTransport(eq(TENANT), eq(NAMESPACE), null);
    }


    @Test
    public void testUpdateTransport() throws Exception {

        updateTransport.name = "my-transport";

        updateTransport.archive = "new-archive";

        updateTransport.processArguments();

        updateTransport.runCmd();

        verify(transports).updateTransport(eq(TransportConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateTransport.name)
                .archive(updateTransport.archive)
                .userConfig(new HashMap<>())
                .build()), eq(updateTransport.archive), eq(new UpdateOptions()));


        updateTransport.archive = null;

        updateTransport.parallelism = 2;

        updateTransport.processArguments();

        updateTransport.updateAuthData = true;

        updateTransport.runCmd();

        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.setUpdateAuthData(true);

        verify(transports).updateTransport(eq(TransportConfig.builder()
                .tenant(PUBLIC_TENANT)
                .namespace(DEFAULT_NAMESPACE)
                .name(updateTransport.name)
                .parallelism(2)
                .userConfig(new HashMap<>())
                .build()), eq(null), eq(updateOptions));


    }


}
