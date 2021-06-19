package org.apache.pulsar.functions.utils;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;

import com.fasterxml.jackson.core.type.TypeReference;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.validator.ConfigValidation;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TransportConfigUtils {

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ExtractedTransportDetails {

        private String sinkClassName;
        private String sourceClassName;
        private String functionClassName;
        private String typeArg;
    }

    public static FunctionDetails convert(TransportConfig transportConfig, ExtractedTransportDetails transportDetails) {
        FunctionDetails.Builder functionDetailsBuilder = convertFunctionDetailsBuilder(transportConfig, transportDetails);
        functionDetailsBuilder.setSink(convertSinkSpecBuilder(transportConfig, transportDetails));
        functionDetailsBuilder.setSource(convertSourceSpecBuilder(transportConfig, transportDetails));
        return functionDetailsBuilder.build();
    }

    public static FunctionDetails.Builder convertFunctionDetailsBuilder(TransportConfig functionConfig, ExtractedTransportDetails functionDetails) {
        boolean isBuiltin = !org.apache.commons.lang3.StringUtils.isEmpty(functionConfig.getJar()) && functionConfig.getJar().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (functionConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(functionConfig.getTenant());
        }
        if (functionConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(functionConfig.getNamespace());
        }
        if (functionConfig.getName() != null) {
            functionDetailsBuilder.setName(functionConfig.getName());
        }
        if (functionConfig.getLogTopic() != null) {
            functionDetailsBuilder.setLogTopic(functionConfig.getLogTopic());
        }
        if (functionConfig.getRuntime() != null) {
            functionDetailsBuilder.setRuntime(FunctionCommon.convertRuntime(functionConfig.getRuntime()));
        }
        if (functionConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    FunctionCommon.convertProcessingGuarantee(functionConfig.getProcessingGuarantees()));
        }


        Map<String, Object> configs = new HashMap<>();
        if (functionConfig.getUserConfig() != null) {
            configs.putAll(functionConfig.getUserConfig());
        }

        // windowing related
        WindowConfig windowConfig = functionConfig.getWindowConfig();
        if (windowConfig != null) {
            windowConfig.setActualWindowFunctionClassName(functionConfig.getFunctionClassName());
            configs.put(WindowConfig.WINDOW_CONFIG_KEY, windowConfig);
            // set class name to window function executor
            functionDetailsBuilder.setClassName("org.apache.pulsar.functions.windowing.WindowFunctionExecutor");

        } else if (functionDetails.getFunctionClassName() != null) {
            functionDetailsBuilder.setClassName(functionDetails.getFunctionClassName());

        } else {
            functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        }

        if (!configs.isEmpty()) {
            functionDetailsBuilder.setUserConfig(new Gson().toJson(configs));
        }

        if (functionConfig.getSecrets() != null && !functionConfig.getSecrets().isEmpty()) {
            functionDetailsBuilder.setSecretsMap(new Gson().toJson(functionConfig.getSecrets()));
        }

        if (functionConfig.getAutoAck() != null) {
            functionDetailsBuilder.setAutoAck(functionConfig.getAutoAck());
        } else {
            functionDetailsBuilder.setAutoAck(true);
        }
        if (functionConfig.getParallelism() != null) {
            functionDetailsBuilder.setParallelism(functionConfig.getParallelism());
        } else {
            functionDetailsBuilder.setParallelism(1);
        }

        // use default resources if resources not set
        Resources resources = Resources.mergeWithDefault(functionConfig.getResources());

        Function.Resources.Builder bldr = Function.Resources.newBuilder();
        bldr.setCpu(resources.getCpu());
        bldr.setRam(resources.getRam());
        bldr.setDisk(resources.getDisk());
        functionDetailsBuilder.setResources(bldr);

        if (!StringUtils.isEmpty(functionConfig.getRuntimeFlags())) {
            functionDetailsBuilder.setRuntimeFlags(functionConfig.getRuntimeFlags());
        }

        functionDetailsBuilder.setComponentType(ComponentType.TRANSPORT);

        if (!StringUtils.isEmpty(functionConfig.getCustomRuntimeOptions())) {
            functionDetailsBuilder.setCustomRuntimeOptions(functionConfig.getCustomRuntimeOptions());
        }

        if (isBuiltin) {
            String builtin = functionConfig.getJar().replaceFirst("^builtin://", "");
            functionDetailsBuilder.setBuiltin(builtin);
        }

        return functionDetailsBuilder;
    }

    public static Function.SinkSpec.Builder convertSinkSpecBuilder(TransportConfig sinkConfig, ExtractedTransportDetails sinkDetails) {
        boolean isBuiltin = !org.apache.commons.lang3.StringUtils.isEmpty(sinkConfig.getArchive()) && sinkConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        // set up sink spec
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();

        if (sinkDetails.getSinkClassName() != null) {
            sinkSpecBuilder.setClassName(sinkDetails.getSinkClassName());
        }

        if (isBuiltin) {
            String builtin = sinkConfig.getArchive().replaceFirst("^builtin://", "");
            sinkSpecBuilder.setBuiltin(builtin);
        }

        if (sinkConfig.getConfigs() != null) {
            sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
        }

        if (sinkDetails.getTypeArg() != null) {
            sinkSpecBuilder.setTypeClassName(sinkDetails.getTypeArg());
        }

        return sinkSpecBuilder;
    }

    public static Function.SourceSpec.Builder convertSourceSpecBuilder(TransportConfig sourceConfig, ExtractedTransportDetails sourceDetails) {
        boolean isBuiltin = !StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        // set source spec
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (sourceDetails.getSourceClassName() != null) {
            sourceSpecBuilder.setClassName(sourceDetails.getSourceClassName());
        }

        if (isBuiltin) {
            String builtin = sourceConfig.getArchive().replaceFirst("^builtin://", "");
            sourceSpecBuilder.setBuiltin(builtin);
        }

        if (sourceConfig.getConfigs() != null) {
            sourceSpecBuilder.setConfigs(new Gson().toJson(sourceConfig.getConfigs()));
        }


        if (sourceDetails.getTypeArg() != null) {
            sourceSpecBuilder.setTypeClassName(sourceDetails.getTypeArg());
        }

        return sourceSpecBuilder;

    }


    public static ExtractedTransportDetails validate(TransportConfig transportConfig, Path archivePath,
            File transportPackageFile, String narExtractionDirectory,
            boolean validateConnectorConfig) {

        if (isEmpty(transportConfig.getTenant())) {
            throw new IllegalArgumentException("Transport tenant cannot be null");
        }
        if (isEmpty(transportConfig.getNamespace())) {
            throw new IllegalArgumentException("Transport namespace cannot be null");
        }
        if (isEmpty(transportConfig.getName())) {
            throw new IllegalArgumentException("Transport name cannot be null");
        }

        if (transportConfig.getParallelism() != null && transportConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Transport parallelism must be a positive number");
        }

        if (transportConfig.getResources() != null) {
            ResourceConfigUtils.validate(transportConfig.getResources());
        }

        if (archivePath == null && transportPackageFile == null) {
            throw new IllegalArgumentException("Transport package is not provided");
        }

        Class<?> typeArg;
        String sinkClassName = transportConfig.getSinkClassName();
        String sourceClassName = transportConfig.getSourceClassName();
        String functionClassName = transportConfig.getFunctionClassName();
        ClassLoader jarClassLoader = null;
        ClassLoader narClassLoader = null;

        Exception jarClassLoaderException = null;
        Exception narClassLoaderException = null;

        try {
            jarClassLoader = ClassLoaderUtils.extractClassLoader(archivePath, transportPackageFile);
        } catch (Exception e) {
            jarClassLoaderException = e;
        }
        try {
            narClassLoader = FunctionCommon.extractNarClassLoader(archivePath, transportPackageFile, narExtractionDirectory);
        } catch (Exception e) {
            narClassLoaderException = e;
        }

        // if sink class name is not provided, we can only try to load archive as a NAR
        if (isEmpty(sinkClassName)) {
            if (narClassLoader == null) {
                throw new IllegalArgumentException("Sink package does not have the correct format. " +
                        "Pulsar cannot determine if the package is a NAR package or JAR package." +
                        "Sink classname is not provided and attempts to load it as a NAR package produced error: "
                        + narClassLoaderException.getMessage());
            }
            try {
                sinkClassName = ConnectorUtils.getIOSinkClass(narClassLoader);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to extract Sink class from archive", e);
            }
            if (validateConnectorConfig) {
                validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
            }
            try {
                typeArg = getSinkType(sinkClassName, narClassLoader);
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException(
                        String.format("Sink class %s must be in class path", sinkClassName), e);
            }

        } else {
            // if sink class name is provided, we need to try to load it as a JAR and as a NAR.
            if (jarClassLoader != null) {
                try {
                    typeArg = getSinkType(sinkClassName, jarClassLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    // class not found in JAR try loading as a NAR and searching for the class
                    if (narClassLoader != null) {
                        try {
                            typeArg = getSinkType(sinkClassName, narClassLoader);
                        } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                            throw new IllegalArgumentException(
                                    String.format("Sink class %s must be in class path", sinkClassName), e1);
                        }
                        if (validateConnectorConfig) {
                            validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Sink class %s must be in class path", sinkClassName), e);
                    }
                }
            } else if (narClassLoader != null) {
                if (validateConnectorConfig) {
                    validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
                }
                try {
                    typeArg = getSinkType(sinkClassName, narClassLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                    throw new IllegalArgumentException(
                            String.format("Sink class %s must be in class path", sinkClassName), e1);
                }
            } else {
                StringBuilder errorMsg = new StringBuilder("Sink package does not have the correct format." +
                        " Pulsar cannot determine if the package is a NAR package or JAR package.");

                if (jarClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a JAR package produced error: " + jarClassLoaderException.getMessage());
                }

                if (narClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a NAR package produced error: " + narClassLoaderException.getMessage());
                }

                throw new IllegalArgumentException(errorMsg.toString());
            }
        }

        // if source class name is not provided, we can only try to load archive as a NAR
        if (isEmpty(sourceClassName)) {
            if (narClassLoader == null) {
                throw new IllegalArgumentException("Source package does not have the correct format. " +
                        "Pulsar cannot determine if the package is a NAR package or JAR package." +
                        "Source classname is not provided and attempts to load it as a NAR package produced the following error.",
                        narClassLoaderException);
            }
            try {
                sourceClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) narClassLoader);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to extract source class from archive", e);
            }
            if (validateConnectorConfig) {
                validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
            }
            try {
                typeArg = getSourceType(sourceClassName, narClassLoader);
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException(
                        String.format("Source class %s must be in class path", sourceClassName), e);
            }

        } else {
            // if source class name is provided, we need to try to load it as a JAR and as a NAR.
            if (jarClassLoader != null) {
                try {
                    typeArg = getSourceType(sourceClassName, jarClassLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    // class not found in JAR try loading as a NAR and searching for the class
                    if (narClassLoader != null) {
                        try {
                            typeArg = getSourceType(sourceClassName, narClassLoader);
                        } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                            throw new IllegalArgumentException(
                                    String.format("Source class %s must be in class path", sourceClassName), e1);
                        }
                        if (validateConnectorConfig) {
                            validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Source class %s must be in class path", sourceClassName), e);
                    }
                }
            } else if (narClassLoader != null) {
                if (validateConnectorConfig) {
                    validateConnectorConfig(transportConfig, (NarClassLoader) narClassLoader);
                }
                try {
                    typeArg = getSourceType(sourceClassName, narClassLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                    throw new IllegalArgumentException(
                            String.format("Source class %s must be in class path", sourceClassName), e1);
                }
            } else {
                StringBuilder errorMsg = new StringBuilder("Source package does not have the correct format." +
                        " Pulsar cannot determine if the package is a NAR package or JAR package.");

                if (jarClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a JAR package produced error: " + jarClassLoaderException.getMessage());
                }

                if (narClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a NAR package produced error: " + narClassLoaderException.getMessage());
                }

                throw new IllegalArgumentException(errorMsg.toString());
            }
        }

        // if function class name is not provided, we can only try to load archive as a NAR
        if (isEmpty(functionClassName)) {
            if (narClassLoader == null) {
                throw new IllegalArgumentException("Function package does not have the correct format. " +
                        "Pulsar cannot determine if the package is a NAR package or JAR package." +
                        "Function classname is not provided and attempts to load it as a NAR package produced the following error.",
                        narClassLoaderException);
            }

            try {
                functionClassName = ConnectorUtils.getIOFunctionClass((NarClassLoader) narClassLoader);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to extract function class from archive", e);
            }
        }

        return new ExtractedTransportDetails(sinkClassName, sourceClassName, functionClassName, typeArg.getName());
    }


    public static void validateConnectorConfig(TransportConfig transportConfig, ClassLoader classLoader) {
        try {
            ConnectorDefinition defn = ConnectorUtils.getConnectorDefinition(classLoader);
            if (defn.getSinkConfigClass() != null) {
                Class configClass = Class.forName(defn.getSinkConfigClass(), true, classLoader);
                Object configObject =
                        ObjectMapperFactory.getThreadLocal().convertValue(transportConfig.getConfigs(), configClass);
                if (configObject != null) {
                    ConfigValidation.validateConfig(configObject);
                }
            }

            if (defn.getSourceConfigClass() != null) {
                Class configClass = Class.forName(defn.getSourceConfigClass(), true, classLoader);
                Object configObject = ObjectMapperFactory.getThreadLocal().convertValue(transportConfig.getConfigs(), configClass);
                if (configObject != null) {
                    ConfigValidation.validateConfig(configObject);
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error validating sink config", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find sink config class", e);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not validate sink config: " + e.getMessage());
        }
    }

    public static TransportConfig convertFromDetails(FunctionDetails functionDetails) {
        TransportConfig transportConfig = new TransportConfig();
        transportConfig.setTenant(functionDetails.getTenant());
        transportConfig.setNamespace(functionDetails.getNamespace());
        transportConfig.setName(functionDetails.getName());
        transportConfig.setParallelism(functionDetails.getParallelism());
        transportConfig.setProcessingGuarantees(FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));


        if (functionDetails.getSource().getSubscriptionType() == Function.SubscriptionType.FAILOVER) {
            transportConfig.setRetainOrdering(true);
            transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        } else {
            transportConfig.setRetainOrdering(false);
            transportConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        }

        transportConfig.setAutoAck(functionDetails.getAutoAck());


        Map<String, Object> userConfig;
        if (!isEmpty(functionDetails.getUserConfig())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            userConfig = new Gson().fromJson(functionDetails.getUserConfig(), type);
        } else {
            userConfig = new HashMap<>();
        }

        if (userConfig.containsKey(WindowConfig.WINDOW_CONFIG_KEY)) {
            WindowConfig windowConfig = new Gson().fromJson(
                    (new Gson().toJson(userConfig.get(WindowConfig.WINDOW_CONFIG_KEY))),
                    WindowConfig.class);
            userConfig.remove(WindowConfig.WINDOW_CONFIG_KEY);
            transportConfig.setFunctionClassName(windowConfig.getActualWindowFunctionClassName());
            transportConfig.setWindowConfig(windowConfig);
        } else {
            transportConfig.setFunctionClassName(functionDetails.getClassName());
        }
        transportConfig.setUserConfig(userConfig);

        if (!isEmpty(functionDetails.getSink().getClassName())) {
            transportConfig.setSinkClassName(functionDetails.getSink().getClassName());
        }

        Function.SourceSpec sourceSpec = functionDetails.getSource();
        if (!StringUtils.isEmpty(sourceSpec.getClassName())) {
            transportConfig.setSourceClassName(sourceSpec.getClassName());
        }

        if (!isEmpty(functionDetails.getSink().getBuiltin())) {
            transportConfig.setArchive("builtin://" + functionDetails.getSink().getBuiltin());
        } else if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
            transportConfig.setArchive("builtin://" + sourceSpec.getBuiltin());
        }

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        Map<String, Object> configMap;
        if (!org.apache.commons.lang3.StringUtils.isEmpty(functionDetails.getSink().getConfigs())) {
            try {
                configMap = ObjectMapperFactory.getThreadLocal().readValue(functionDetails.getSink().getConfigs(), typeRef);
            } catch (IOException e) {
                log.error("Failed to read configs for sink {}", FunctionCommon.getFullyQualifiedName(functionDetails), e);
                throw new RuntimeException(e);
            }
            transportConfig.setConfigs(configMap);
        } else if (!org.apache.commons.lang3.StringUtils.isEmpty(functionDetails.getSource().getConfigs())) {
            try {
                configMap = ObjectMapperFactory.getThreadLocal().readValue(functionDetails.getSource().getConfigs(), typeRef);
            } catch (IOException e) {
                log.error("Failed to read configs for sink {}", FunctionCommon.getFullyQualifiedName(functionDetails), e);
                throw new RuntimeException(e);
            }
            transportConfig.setConfigs(configMap);
        }

        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            transportConfig.setSecrets(secretsMap);
        }

        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
        }

        if (isNotBlank(functionDetails.getRuntimeFlags())) {
            transportConfig.setRuntimeFlags(functionDetails.getRuntimeFlags());
        }

        if (!isEmpty(functionDetails.getCustomRuntimeOptions())) {
            transportConfig.setCustomRuntimeOptions(functionDetails.getCustomRuntimeOptions());
        }


        return transportConfig;
    }

    public static TransportConfig validateUpdate(TransportConfig existingConfig, TransportConfig newConfig) {
        TransportConfig mergedConfig = existingConfig.toBuilder().build();

        if (!existingConfig.getTenant().equals(newConfig.getTenant())) {
            throw new IllegalArgumentException("Tenants differ");
        }
        if (!existingConfig.getNamespace().equals(newConfig.getNamespace())) {
            throw new IllegalArgumentException("Namespaces differ");
        }
        if (!existingConfig.getName().equals(newConfig.getName())) {
            throw new IllegalArgumentException("Transport Names differ");
        }
        if (!StringUtils.isEmpty(newConfig.getSinkClassName())) {
            mergedConfig.setSinkClassName(newConfig.getSinkClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getSourceClassName())) {
            mergedConfig.setSourceClassName(newConfig.getSourceClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getFunctionClassName())) {
            mergedConfig.setFunctionClassName(newConfig.getFunctionClassName());
        }

        if (!StringUtils.isEmpty(newConfig.getJar())) {
            mergedConfig.setJar(newConfig.getJar());
        }

        if (newConfig.getConfigs() != null) {
            mergedConfig.setConfigs(newConfig.getConfigs());
        }
        if (newConfig.getSecrets() != null) {
            mergedConfig.setSecrets(newConfig.getSecrets());
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees().equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantees cannot be altered");
        }

        if (newConfig.getAutoAck() != null && !newConfig.getAutoAck().equals(existingConfig.getAutoAck())) {
            throw new IllegalArgumentException("AutoAck cannot be altered");
        }

        if (newConfig.getRetainOrdering() != null && !newConfig.getRetainOrdering().equals(existingConfig.getRetainOrdering())) {
            throw new IllegalArgumentException("Retain Ordering cannot be altered");
        }

        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getResources() != null) {
            mergedConfig.setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
        }
        if (!StringUtils.isEmpty(newConfig.getArchive())) {
            mergedConfig.setArchive(newConfig.getArchive());
        }
        if (!StringUtils.isEmpty(newConfig.getRuntimeFlags())) {
            mergedConfig.setRuntimeFlags(newConfig.getRuntimeFlags());
        }
        if (!StringUtils.isEmpty(newConfig.getCustomRuntimeOptions())) {
            mergedConfig.setCustomRuntimeOptions(newConfig.getCustomRuntimeOptions());
        }

        if (newConfig.getUserConfig() != null) {
            mergedConfig.setUserConfig(newConfig.getUserConfig());
        }

        if (newConfig.getWindowConfig() != null) {
            mergedConfig.setWindowConfig(newConfig.getWindowConfig());
        }

        return mergedConfig;
    }
}
