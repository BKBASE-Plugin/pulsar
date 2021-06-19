/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO transports (egress data from Pulsar)")
@Slf4j
public class CmdTransports extends CmdBase {

    private final CreateTransport createTransport;
    private final UpdateTransport updateTransport;
    private final DeleteTransport deleteTransport;
    private final ListTransports listTransports;
    private final GetTransport getTransport;
    private final GetTransportStatus getTransportStatus;
    private final StopTransport stopTransport;
    private final StartTransport startTransport;
    private final RestartTransport restartTransport;
    private final LocalTransportRunner localTransportRunner;

    public CmdTransports(PulsarAdmin admin) {
        super("transports", admin);
        createTransport = new CreateTransport();
        updateTransport = new UpdateTransport();
        deleteTransport = new DeleteTransport();
        listTransports = new ListTransports();
        getTransport = new GetTransport();
        getTransportStatus = new GetTransportStatus();
        stopTransport = new StopTransport();
        startTransport = new StartTransport();
        restartTransport = new RestartTransport();
        localTransportRunner = new LocalTransportRunner();

        jcommander.addCommand("create", createTransport);
        jcommander.addCommand("update", updateTransport);
        jcommander.addCommand("delete", deleteTransport);
        jcommander.addCommand("list", listTransports);
        jcommander.addCommand("get", getTransport);
        // TODO deprecate getstatus
        jcommander.addCommand("status", getTransportStatus, "getstatus");
        jcommander.addCommand("stop", stopTransport);
        jcommander.addCommand("start", startTransport);
        jcommander.addCommand("restart", restartTransport);
        jcommander.addCommand("localrun", localTransportRunner);
        jcommander.addCommand("available-transports", new ListBuiltInTransports());
        jcommander.addCommand("reload", new ReloadBuiltInTransports());
    }

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {

        @Override
        void run() throws Exception {
            try {
                processArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println();
                String chosenCommand = jcommander.getParsedCommand();
                jcommander.usage(chosenCommand);
                return;
            }
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    @Parameters(commandDescription = "Run a Pulsar IO transport connector locally (rather than deploying it to the Pulsar cluster)")
    protected class LocalTransportRunner extends CreateTransport {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String DEPRECATED_brokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker", hidden = true)
        protected String DEPRECATED_clientAuthPlugin;
        @Parameter(names = "--client-auth-plugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;

        @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String DEPRECATED_clientAuthParams;
        @Parameter(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;


        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean DEPRECATED_tlsAllowInsecureConnection;
        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection")
        protected boolean tlsAllowInsecureConnection;

        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification", hidden = true)
        protected Boolean DEPRECATED_tlsHostNameVerificationEnabled;
        @Parameter(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;

        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String DEPRECATED_tlsTrustCertFilePath;
        @Parameter(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_brokerServiceUrl)) {
                brokerServiceUrl = DEPRECATED_brokerServiceUrl;
            }
            if (!StringUtils.isBlank(DEPRECATED_clientAuthPlugin)) {
                clientAuthPlugin = DEPRECATED_clientAuthPlugin;
            }
            if (!StringUtils.isBlank(DEPRECATED_clientAuthParams)) {
                clientAuthParams = DEPRECATED_clientAuthParams;
            }
            if (DEPRECATED_tlsAllowInsecureConnection != null) {
                tlsAllowInsecureConnection = DEPRECATED_tlsAllowInsecureConnection;
            }
            if (DEPRECATED_tlsHostNameVerificationEnabled != null) {
                tlsHostNameVerificationEnabled = DEPRECATED_tlsHostNameVerificationEnabled;
            }
            if (!StringUtils.isBlank(DEPRECATED_tlsTrustCertFilePath)) {
                tlsTrustCertFilePath = DEPRECATED_tlsTrustCertFilePath;
            }
        }

        @Override
        public void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--transportConfig");
            localRunArgs.add(new Gson().toJson(transportConfig));
            for (Field field : this.getClass().getDeclaredFields()) {
                if (field.getName().startsWith("DEPRECATED")) {
                    continue;
                }
                if (field.getName().contains("$")) {
                    continue;
                }
                Object value = field.get(this);
                if (value != null) {
                    localRunArgs.add("--" + field.getName());
                    localRunArgs.add(value.toString());
                }
            }
            ProcessBuilder processBuilder = new ProcessBuilder(localRunArgs).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }

        @Override
        protected String validateTransportType(String transportType) {
            return transportType;
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO transport connector to run in a Pulsar cluster")
    protected class CreateTransport extends TransportDetailsCommand {

        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.transports().createTransportWithUrl(transportConfig, transportConfig.getArchive());
            } else {
                admin.transports().createTransport(transportConfig, transportConfig.getArchive());
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO transport connector")
    protected class UpdateTransport extends TransportDetailsCommand {

        @Parameter(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

        @Override
        void runCmd() throws Exception {
            UpdateOptions updateOptions = new UpdateOptions();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.transports().updateTransportWithUrl(transportConfig, transportConfig.getArchive(), updateOptions);
            } else {
                admin.transports().updateTransport(transportConfig, transportConfig.getArchive(), updateOptions);
            }
            print("Updated successfully");
        }

        protected void validateTransportConfigs(TransportConfig transportConfig) {
            if (transportConfig.getTenant() == null) {
                transportConfig.setTenant(PUBLIC_TENANT);
            }
            if (transportConfig.getNamespace() == null) {
                transportConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }
    }

    abstract class TransportDetailsCommand extends BaseCommand {

        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

        @Parameter(names = "--tenant", description = "The transport's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The transport's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The transport's name")
        protected String name;

        @Parameter(names = "--jar", description = "Path to the JAR file for the function (if the function is written in Java). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(
                names = "--py",
                description = "Path to the main Python file/Python Wheel file for the function (if the function is written in Python)",
                listConverter = StringConverter.class)
        protected String pyFile;
        @Parameter(
                names = "--go",
                description = "Path to the main Go executable binary for the function (if the function is written in Go)")
        protected String goFile;

        @Parameter(names = {"-t", "--transport-type"}, description = "The transport's connector provider")
        protected String transportType;

        // for backwards compatibility purposes
        @Parameter(names = "--logTopic", description = "The topic to which the logs of a Pulsar Function are produced", hidden = true)
        protected String DEPRECATED_logTopic;
        @Parameter(names = "--log-topic", description = "The topic to which the logs of a Pulsar Function are produced")
        protected String logTopic;


        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the transport", hidden = true)
        protected FunctionConfig.ProcessingGuarantees DEPRECATED_processingGuarantees;
        @Parameter(names = "--processing-guarantees", description = "The processing guarantees (aka delivery semantics) applied to the transport")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--retainOrdering", description = "Transport consumes and transports messages in order", hidden = true)
        protected Boolean DEPRECATED_retainOrdering;
        @Parameter(names = "--retain-ordering", description = "Transport consumes and transports messages in order")
        protected Boolean retainOrdering;
        @Parameter(names = "--parallelism", description = "The transport's parallelism factor (i.e. the number of transport instances to run)")
        protected Integer parallelism;
        @Parameter(names = {"-a",
                "--archive"}, description = "Path to the archive file for the transport. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String archive;

        @Parameter(names = "--sinkClassName", description = "The transport's class name if archive is file-url-path (file://)")
        protected String sinkClassName;

        @Parameter(names = "--sourceClassName", description = "The transport's class name if archive is file-url-path (file://)")
        protected String sourceClassName;

        @Parameter(names = "--functionClassName", description = "The transport's class name if archive is file-url-path (file://)")
        protected String functionClassName;

        @Parameter(names = "--transportConfigFile", description = "The path to a YAML config file specifying the "
                + "Transport's configuration", hidden = true)
        protected String DEPRECATED_transportConfigFile;
        @Parameter(names = "--transport-config-file", description = "The path to a YAML config file specifying the "
                + "Transport's configuration")
        protected String transportConfigFile;

        // for backwards compatibility purposes
        @Parameter(names = "--userConfig", description = "User-defined config key/values", hidden = true)
        protected String DEPRECATED_userConfigString;
        @Parameter(names = "--user-config", description = "User-defined config key/values")
        protected String userConfigString;

        @Parameter(names = "--cpu", description = "The CPU (in cores) that needs to be allocated per transport instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The RAM (in bytes) that need to be allocated per transport instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk (in bytes) that need to be allocated per transport instance (applicable only to Docker runtime)")
        protected Long disk;
        @Parameter(names = "--transportConfig", description = "User defined configs key/values", hidden = true)
        protected String DEPRECATED_transportConfigString;
        @Parameter(names = "--transport-config", description = "User defined configs key/values")
        protected String transportConfigString;
        @Parameter(names = "--auto-ack", description = "Whether or not the framework will automatically acknowledge messages", arity = 1)
        protected Boolean autoAck;
        @Parameter(names = "--custom-runtime-options", description = "A string that encodes options to customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;

        protected TransportConfig transportConfig;

        private void mergeArgs() {
            if (DEPRECATED_processingGuarantees != null) {
                processingGuarantees = DEPRECATED_processingGuarantees;
            }
            if (DEPRECATED_retainOrdering != null) {
                retainOrdering = DEPRECATED_retainOrdering;
            }
            if (!StringUtils.isBlank(DEPRECATED_transportConfigFile)) {
                transportConfigFile = DEPRECATED_transportConfigFile;
            }
            if (!StringUtils.isBlank(DEPRECATED_transportConfigString)) {
                transportConfigString = DEPRECATED_transportConfigString;
            }
            if (!StringUtils.isBlank(DEPRECATED_logTopic)) {
                logTopic = DEPRECATED_logTopic;
            }
            if (!StringUtils.isBlank(DEPRECATED_userConfigString)) {
                userConfigString = DEPRECATED_userConfigString;
            }


        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

            if (null != transportConfigFile) {
                this.transportConfig = CmdUtils.loadConfig(transportConfigFile, TransportConfig.class);
            } else {
                this.transportConfig = new TransportConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, transportConfig);
            } else {
                if (null != tenant) {
                    transportConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    transportConfig.setNamespace(namespace);
                }
                if (null != name) {
                    transportConfig.setName(name);
                }
            }

            if (null != sinkClassName) {
                transportConfig.setSinkClassName(sinkClassName);
            }

            if (null != sourceClassName) {
                transportConfig.setSourceClassName(sourceClassName);
            }

            if (null != functionClassName) {
                transportConfig.setFunctionClassName(functionClassName);
            }

            if (null != processingGuarantees) {
                transportConfig.setProcessingGuarantees(processingGuarantees);
            }

            if (null != logTopic) {
                transportConfig.setLogTopic(logTopic);
            }

            if (retainOrdering != null) {
                transportConfig.setRetainOrdering(retainOrdering);
            }


            if (parallelism != null) {
                transportConfig.setParallelism(parallelism);
            }

            if (archive != null && transportType != null) {
                throw new ParameterException("Cannot specify both archive and transport-type");
            }

            if (null != archive) {
                transportConfig.setArchive(archive);
            }

            if (transportType != null) {
                transportConfig.setArchive(validateTransportType(transportType));
            }

            Resources resources = transportConfig.getResources();
            if (cpu != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setCpu(cpu);
            }

            if (ram != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setRam(ram);
            }

            if (disk != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setDisk(disk);
            }
            if (resources != null) {
                transportConfig.setResources(resources);
            }

            if (null != transportConfigString) {
                transportConfig.setConfigs(parseConfigs(transportConfigString));
            }

            if (autoAck != null) {
                transportConfig.setAutoAck(autoAck);
            }


            if (customRuntimeOptions != null) {
                transportConfig.setCustomRuntimeOptions(customRuntimeOptions);
            }

            if (null != jarFile) {
                transportConfig.setJar(jarFile);
            }

            if (null != pyFile) {
                transportConfig.setPy(pyFile);
            }

            if (null != goFile) {
                transportConfig.setGo(goFile);
            }

            if (null != userConfigString) {
                Type type = new TypeToken<Map<String, String>>() {
                }.getType();
                Map<String, Object> userConfigMap = new Gson().fromJson(userConfigString, type);
                transportConfig.setUserConfig(userConfigMap);
            }
            if (transportConfig.getUserConfig() == null) {
                transportConfig.setUserConfig(new HashMap<>());
            }

            // check if configs are valid
            validateTransportConfigs(transportConfig);
        }

        protected Map<String, Object> parseConfigs(String str) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            return new Gson().fromJson(str, type);
        }

        protected void validateTransportConfigs(TransportConfig transportConfig) {

            if (isBlank(transportConfig.getArchive())) {
                throw new ParameterException("Transport archive not specfied");
            }

            Utils.inferMissingArguments(transportConfig);

            if (!Utils.isFunctionPackageUrlSupported(transportConfig.getArchive()) &&
                    !transportConfig.getArchive().startsWith(Utils.BUILTIN)) {
                if (!new File(transportConfig.getArchive()).exists()) {
                    throw new IllegalArgumentException(String.format("Transport Archive file %s does not exist", transportConfig.getArchive()));
                }
            }
        }

        protected String validateTransportType(String transportType) throws IOException {
            Set<String> availableTransports;
            try {
                availableTransports = admin.transports().getBuiltInTransports().stream().map(ConnectorDefinition::getName).collect(Collectors.toSet());
            } catch (PulsarAdminException e) {
                throw new IOException(e);
            }

            if (!availableTransports.contains(transportType)) {
                throw new ParameterException(
                        "Invalid transport type '" + transportType + "' -- Available transports are: " + availableTransports);
            }

            // Source type is a valid built-in connector type
            return "builtin://" + transportType;
        }
    }

    /**
     * Transport level command
     */
    @Getter
    abstract class TransportCommand extends BaseCommand {

        @Parameter(names = "--tenant", description = "The transport's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The transport's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The transport's name")
        protected String transportName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
            if (null == transportName) {
                throw new RuntimeException(
                        "You must specify a name for the transport");
            }
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar IO transport connector")
    protected class DeleteTransport extends TransportCommand {

        @Override
        void runCmd() throws Exception {
            admin.transports().deleteTransport(tenant, namespace, transportName);
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Gets the information about a Pulsar IO transport connector")
    protected class GetTransport extends TransportCommand {

        @Override
        void runCmd() throws Exception {
            TransportConfig transportConfig = admin.transports().getTransport(tenant, namespace, transportName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(transportConfig));
        }
    }

    /**
     * List Sources command
     */
    @Parameters(commandDescription = "List all running Pulsar IO transport connectors")
    protected class ListTransports extends BaseCommand {

        @Parameter(names = "--tenant", description = "The transport's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The transport's namespace")
        protected String namespace;

        @Override
        public void processArguments() {
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
        }

        @Override
        void runCmd() throws Exception {
            List<String> transports = admin.transports().listTransports(tenant, namespace);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(transports));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar transport")
    class GetTransportStatus extends TransportCommand {

        @Parameter(names = "--instance-id", description = "The transport instanceId (Get-status of all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(admin.transports().getTransportStatus(tenant, namespace, transportName));
            } else {
                print(admin.transports().getTransportStatus(tenant, namespace, transportName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Restart transport instance")
    class RestartTransport extends TransportCommand {

        @Parameter(names = "--instance-id", description = "The transport instanceId (restart all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.transports().restartTransport(tenant, namespace, transportName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.transports().restartTransport(tenant, namespace, transportName);
            }
            System.out.println("Restarted successfully");
        }
    }

    @Parameters(commandDescription = "Stops transport instance")
    class StopTransport extends TransportCommand {

        @Parameter(names = "--instance-id", description = "The transport instanceId (stop all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.transports().stopTransport(tenant, namespace, transportName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.transports().stopTransport(tenant, namespace, transportName);
            }
            System.out.println("Stopped successfully");
        }
    }

    @Parameters(commandDescription = "Starts transport instance")
    class StartTransport extends TransportCommand {

        @Parameter(names = "--instance-id", description = "The transport instanceId (start all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    admin.transports().startTransport(tenant, namespace, transportName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                admin.transports().startTransport(tenant, namespace, transportName);
            }
            System.out.println("Started successfully");
        }
    }

    @Parameters(commandDescription = "Get the list of Pulsar IO connector transports supported by Pulsar cluster")
    public class ListBuiltInTransports extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            admin.transports().getBuiltInTransports().stream().filter(x -> isNotBlank(x.getSinkClass()) && isNotBlank(x.getSourceClass()))
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }

    @Parameters(commandDescription = "Reload the available built-in connectors")
    public class ReloadBuiltInTransports extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            admin.transports().reloadBuiltInTransports();
        }
    }

    private void parseFullyQualifiedFunctionName(String fqfn, TransportConfig transportConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new ParameterException("Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
        } else {
            transportConfig.setTenant(args[0]);
            transportConfig.setNamespace(args[1]);
            transportConfig.setName(args[2]);
        }
    }
}
