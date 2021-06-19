package org.apache.pulsar.functions.worker.rest.api;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.functions.auth.FunctionAuthUtils.getFunctionAuthData;
import static org.apache.pulsar.functions.worker.WorkerUtils.isFunctionCodeBuiltin;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.TransportStatus;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.auth.FunctionAuthData;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentTypeUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.TransportConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Slf4j
public class TransportsImpl extends ComponentImpl {

    public TransportsImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, Function.FunctionDetails.ComponentType.TRANSPORT);
    }


    public void registerTransport(final String tenant,
            final String namespace,
            final String transportName,
            final InputStream uploadedInputStream,
            final FormDataContentDisposition fileDetail,
            final String connectorPkgUrl,
            final TransportConfig transportConfig,
            final String clientRole,
            AuthenticationDataHttps clientAuthenticationDataHttps) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (transportName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Transport name is not provided");
        }
        if (transportConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Transport config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not authorized to register {}", tenant, namespace,
                        transportName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, transportName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        try {
            // Check tenant exists
            worker().getBrokerAdmin().tenants().getTenantInfo(tenant);

            String qualifiedNamespace = tenant + "/" + namespace;
            List<String> namespaces = worker().getBrokerAdmin().namespaces().getNamespaces(tenant);
            if (namespaces != null && !namespaces.contains(qualifiedNamespace)) {
                String qualifiedNamespaceWithCluster = String.format("%s/%s/%s", tenant,
                        worker().getWorkerConfig().getPulsarFunctionsCluster(), namespace);
                if (namespaces != null && !namespaces.contains(qualifiedNamespaceWithCluster)) {
                    log.error("{}/{}/{} Namespace {} does not exist", tenant, namespace, transportName, namespace);
                    throw new RestException(Response.Status.BAD_REQUEST, "Namespace does not exist");
                }
            }
        } catch (PulsarAdminException.NotAuthorizedException e) {
            log.error("{}/{}/{} Client [{}] is not authorized to operate {} on tenant", tenant, namespace,
                    transportName, clientRole, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");
        } catch (PulsarAdminException.NotFoundException e) {
            log.error("{}/{}/{} Tenant {} does not exist", tenant, namespace, transportName, tenant);
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant does not exist");
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Issues getting tenant data", tenant, namespace, transportName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (functionMetaDataManager.containsFunction(tenant, namespace, transportName)) {
            log.error("{} {}/{}/{} already exists", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName);
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s already exists", ComponentTypeUtils.toString(componentType), transportName));
        }

        Function.FunctionDetails functionDetails = null;
        boolean isPkgUrlProvided = isNotBlank(connectorPkgUrl);
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isPkgUrlProvided) {

                    if (!Utils.isFunctionPackageUrlSupported(connectorPkgUrl)) {
                        throw new IllegalArgumentException("Function Package url is not valid. supported url (http/https/file)");
                    }
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(connectorPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), connectorPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            transportConfig, componentPackageFile);
                } else {
                    if (uploadedInputStream != null) {
                        componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            transportConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                }
            } catch (Exception e) {
                log.error("Invalid register {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("{} {}/{}/{} cannot be admitted by the runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s", ComponentTypeUtils.toString(componentType), transportName, e.getMessage()));
            }

            // function state
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder()
                    .setFunctionDetails(functionDetails)
                    .setCreateTime(System.currentTimeMillis())
                    .setVersion(0);

            // cache auth if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                Function.FunctionDetails finalFunctionDetails = functionDetails;
                worker().getFunctionRuntimeManager()
                        .getRuntimeFactory()
                        .getAuthProvider().ifPresent(functionAuthProvider -> {
                    if (clientAuthenticationDataHttps != null) {

                        try {
                            Optional<FunctionAuthData> functionAuthData = functionAuthProvider
                                    .cacheAuthData(finalFunctionDetails, clientAuthenticationDataHttps);

                            functionAuthData.ifPresent(authData -> functionMetaDataBuilder.setFunctionAuthSpec(
                                    Function.FunctionAuthenticationSpec.newBuilder()
                                            .setData(ByteString.copyFrom(authData.getData()))
                                            .build()));
                        } catch (Exception e) {
                            log.error("Error caching authentication data for {} {}/{}/{}",
                                    ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);

                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s",
                                    ComponentTypeUtils.toString(componentType), transportName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            try {
                packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                        connectorPkgUrl, fileDetail, componentPackageFile);
            } catch (Exception e) {
                log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);
                throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);
            updateRequest(functionMetaDataBuilder.build());
        } finally {

            if (!(connectorPkgUrl != null && connectorPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }

    public void updateTransport(final String tenant,
            final String namespace,
            final String transportName,
            final InputStream uploadedInputStream,
            final FormDataContentDisposition fileDetail,
            final String transportPkgUrl,
            final TransportConfig transportConfig,
            final String clientRole,
            AuthenticationDataHttps clientAuthenticationDataHttps,
            UpdateOptions updateOptions) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (tenant == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Tenant is not provided");
        }
        if (namespace == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Namespace is not provided");
        }
        if (transportName == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Transport name is not provided");
        }
        if (transportConfig == null) {
            throw new RestException(Response.Status.BAD_REQUEST, "Transport config is not provided");
        }

        try {
            if (!isAuthorizedRole(tenant, namespace, clientRole, clientAuthenticationDataHttps)) {
                log.error("{}/{}/{} Client [{}] is not authorized to update {}", tenant, namespace,
                        transportName, clientRole, ComponentTypeUtils.toString(componentType));
                throw new RestException(Response.Status.UNAUTHORIZED, "client is not authorize to perform operation");

            }
        } catch (PulsarAdminException e) {
            log.error("{}/{}/{} Failed to authorize [{}]", tenant, namespace, transportName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();

        if (!functionMetaDataManager.containsFunction(tenant, namespace, transportName)) {
            throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), transportName));
        }

        Function.FunctionMetaData existingComponent = functionMetaDataManager.getFunctionMetaData(tenant, namespace, transportName);

        if (!InstanceUtils.calculateSubjectType(existingComponent.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, transportName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format("%s %s doesn't exist", ComponentTypeUtils.toString(componentType), transportName));
        }

        TransportConfig existingTransportConfig = TransportConfigUtils.convertFromDetails(existingComponent.getFunctionDetails());
        // The rest end points take precedence over whatever is there in functionconfig
        transportConfig.setTenant(tenant);
        transportConfig.setNamespace(namespace);
        transportConfig.setName(transportName);

        TransportConfig mergedConfig;
        try {
            mergedConfig = TransportConfigUtils.validateUpdate(existingTransportConfig, transportConfig);
        } catch (Exception e) {
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        if (existingTransportConfig.equals(mergedConfig) && isBlank(transportPkgUrl) && uploadedInputStream == null) {
            log.error("{}/{}/{} Update contains no changes", tenant, namespace, transportName);
            throw new RestException(Response.Status.BAD_REQUEST, "Update contains no change");
        }

        Function.FunctionDetails functionDetails = null;
        File componentPackageFile = null;
        try {

            // validate parameters
            try {
                if (isNotBlank(transportPkgUrl)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(transportPkgUrl);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), transportPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.FILE)
                        || existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.HTTP)) {
                    try {
                        componentPackageFile = FunctionCommon.extractFileFromPkgURL(existingComponent.getPackageLocation().getPackagePath());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(String.format("Encountered error \"%s\" when getting %s package from %s", e.getMessage(), ComponentTypeUtils.toString(componentType), transportPkgUrl));
                    }
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            mergedConfig, componentPackageFile);
                } else if (uploadedInputStream != null) {

                    componentPackageFile = WorkerUtils.dumpToTmpFile(uploadedInputStream);
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            mergedConfig, componentPackageFile);

                } else if (existingComponent.getPackageLocation().getPackagePath().startsWith(Utils.BUILTIN)) {
                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            mergedConfig, componentPackageFile);
                    if (!isFunctionCodeBuiltin(functionDetails) && (componentPackageFile == null || fileDetail == null)) {
                        throw new IllegalArgumentException(ComponentTypeUtils.toString(componentType) + " Package is not provided");
                    }
                } else {

                    componentPackageFile = FunctionCommon.createPkgTempFile();
                    componentPackageFile.deleteOnExit();
                    WorkerUtils.downloadFromBookkeeper(worker().getDlogNamespace(), componentPackageFile, existingComponent.getPackageLocation().getPackagePath());

                    functionDetails = validateUpdateRequestParams(tenant, namespace, transportName,
                            mergedConfig, componentPackageFile);
                }
            } catch (Exception e) {
                log.error("Invalid update {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);
                throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
            }

            try {
                worker().getFunctionRuntimeManager().getRuntimeFactory().doAdmissionChecks(functionDetails);
            } catch (Exception e) {
                log.error("Updated {} {}/{}/{} cannot be submitted to runtime factory", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName);
                throw new RestException(Response.Status.BAD_REQUEST, String.format("%s %s cannot be admitted:- %s",
                        ComponentTypeUtils.toString(componentType), transportName, e.getMessage()));
            }

            // merge from existing metadata
            Function.FunctionMetaData.Builder functionMetaDataBuilder = Function.FunctionMetaData.newBuilder().mergeFrom(existingComponent)
                    .setFunctionDetails(functionDetails);

            // update auth data if need
            if (worker().getWorkerConfig().isAuthenticationEnabled()) {
                Function.FunctionDetails finalFunctionDetails = functionDetails;
                worker().getFunctionRuntimeManager()
                        .getRuntimeFactory()
                        .getAuthProvider().ifPresent(functionAuthProvider -> {
                    if (clientAuthenticationDataHttps != null && updateOptions != null && updateOptions.isUpdateAuthData()) {
                        // get existing auth data if it exists
                        Optional<FunctionAuthData> existingFunctionAuthData = Optional.empty();
                        if (functionMetaDataBuilder.hasFunctionAuthSpec()) {
                            existingFunctionAuthData = Optional.ofNullable(getFunctionAuthData(Optional.ofNullable(functionMetaDataBuilder.getFunctionAuthSpec())));
                        }

                        try {
                            Optional<FunctionAuthData> newFunctionAuthData = functionAuthProvider
                                    .updateAuthData(finalFunctionDetails, existingFunctionAuthData,
                                            clientAuthenticationDataHttps);

                            if (newFunctionAuthData.isPresent()) {
                                functionMetaDataBuilder.setFunctionAuthSpec(
                                        Function.FunctionAuthenticationSpec.newBuilder()
                                                .setData(ByteString.copyFrom(newFunctionAuthData.get().getData()))
                                                .build());
                            } else {
                                functionMetaDataBuilder.clearFunctionAuthSpec();
                            }
                        } catch (Exception e) {
                            log.error("Error updating authentication data for {} {}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);
                            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, String.format("Error caching authentication data for %s %s:- %s", ComponentTypeUtils.toString(componentType), transportName, e.getMessage()));
                        }
                    }
                });
            }

            Function.PackageLocationMetaData.Builder packageLocationMetaDataBuilder;
            if (isNotBlank(transportPkgUrl) || uploadedInputStream != null) {
                try {
                    packageLocationMetaDataBuilder = getFunctionPackageLocation(functionMetaDataBuilder.build(),
                            transportPkgUrl, fileDetail, componentPackageFile);
                } catch (Exception e) {
                    log.error("Failed process {} {}/{}/{} package: ", ComponentTypeUtils.toString(componentType), tenant, namespace, transportName, e);
                    throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
                }
            } else {
                packageLocationMetaDataBuilder = Function.PackageLocationMetaData.newBuilder().mergeFrom(existingComponent.getPackageLocation());
            }

            functionMetaDataBuilder.setPackageLocation(packageLocationMetaDataBuilder);

            updateRequest(functionMetaDataBuilder.build());
        } finally {
            if (!(transportPkgUrl != null && transportPkgUrl.startsWith(Utils.FILE))
                    && componentPackageFile != null && componentPackageFile.exists()) {
                componentPackageFile.delete();
            }
        }
    }


    private Function.FunctionDetails validateUpdateRequestParams(final String tenant,
            final String namespace,
            final String transportName,
            final TransportConfig transportConfig,
            final File transportPackageFile) throws IOException {

        Path archivePath = null;
        // The rest end points take precedence over whatever is there in sourceconfig
        transportConfig.setTenant(tenant);
        transportConfig.setNamespace(namespace);
        transportConfig.setName(transportName);
        org.apache.pulsar.common.functions.Utils.inferMissingArguments(transportConfig);
        if (!StringUtils.isEmpty(transportConfig.getArchive())) {
            String builtinArchive = transportConfig.getArchive();
            if (builtinArchive.startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
                builtinArchive = builtinArchive.replaceFirst("^builtin://", "");
            }
            try {
                archivePath = this.worker().getConnectorsManager().getTransportArchive(builtinArchive);
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("No Transport archive %s found", archivePath));
            }
        }

        TransportConfigUtils.ExtractedTransportDetails sourceDetails = TransportConfigUtils.validate(transportConfig, archivePath,
                transportPackageFile, worker().getWorkerConfig().getNarExtractionDirectory(),
                worker().getWorkerConfig().getValidateConnectorConfig());
        return TransportConfigUtils.convert(transportConfig, sourceDetails);
    }

    public TransportConfig getTransportInfo(final String tenant,
            final String namespace,
            final String componentName) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", ComponentTypeUtils.toString(componentType), tenant, namespace, componentName);
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }

        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, ComponentTypeUtils.toString(componentType));
            throw new RestException(Response.Status.NOT_FOUND, String.format(ComponentTypeUtils.toString(componentType) + " %s doesn't exist", componentName));
        }

        TransportConfig config = TransportConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }


    public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData getTransportInstanceStatus(final String tenant,
            final String namespace,
            final String transportName,
            final String instanceId,
            final URI uri,
            final String clientRole,
            final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, transportName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);

        TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData;
        try {
            transportInstanceStatusData = new GetTransportStatus().getComponentInstanceStatus(tenant, namespace, transportName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, transportName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return transportInstanceStatusData;
    }

    public TransportStatus getTransportStatus(final String tenant,
            final String namespace,
            final String componentName,
            final URI uri,
            final String clientRole,
            final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

        TransportStatus transportStatus;
        try {
            transportStatus = new GetTransportStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return transportStatus;
    }



    private class GetTransportStatus extends GetStatus<TransportStatus, TransportStatus.TransportInstanceStatus.TransportInstanceStatusData> {

        @Override
        public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData notScheduledInstance() {
            TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData = new TransportStatus.TransportInstanceStatus.TransportInstanceStatusData();
            transportInstanceStatusData.setRunning(false);
            transportInstanceStatusData.setError("Transport has not been scheduled");
            return transportInstanceStatusData;
        }

        @Override
        public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData = new TransportStatus.TransportInstanceStatus.TransportInstanceStatusData();
            transportInstanceStatusData.setRunning(status.getRunning());
            transportInstanceStatusData.setError(status.getFailureException());
            transportInstanceStatusData.setNumRestarts(status.getNumRestarts());
            transportInstanceStatusData.setNumReadFromPulsar(status.getNumReceived());
            transportInstanceStatusData.setNumReceivedFromSource(status.getNumReceived());
            transportInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
            transportInstanceStatusData.setNumReceived(status.getNumReceived());
            transportInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());
            // We treat source/user/system exceptions returned from function as system exceptions
            transportInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumUserExceptions() + status.getNumSourceExceptions());

            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            transportInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            transportInstanceStatusData.setNumSinkExceptions(status.getNumSinkExceptions());
            transportInstanceStatusData.setNumSourceExceptions(status.getNumSourceExceptions());

            List<ExceptionInformation> sinkExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                sinkExceptionInformationList.add(exceptionInformation);
            }

            transportInstanceStatusData.setLatestSinkExceptions(sinkExceptionInformationList);
            List<ExceptionInformation> sourceExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                sourceExceptionInformationList.add(exceptionInformation);
            }

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                userExceptionInformationList.add(exceptionInformation);
            }
            transportInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

            transportInstanceStatusData.setLatestSourceExceptions(sourceExceptionInformationList);

            transportInstanceStatusData.setNumWrittenToSink(status.getNumSuccessfullyProcessed());
            transportInstanceStatusData.setNumWritten(status.getNumSuccessfullyProcessed());
            transportInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
            transportInstanceStatusData.setAverageLatency(status.getAverageLatency());
            transportInstanceStatusData.setWorkerId(assignedWorkerId);

            return transportInstanceStatusData;
        }

        @Override
        public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData notRunning(String assignedWorkerId, String error) {
            TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData = new TransportStatus.TransportInstanceStatus.TransportInstanceStatusData();
            transportInstanceStatusData.setRunning(false);
            if (error != null) {
                transportInstanceStatusData.setError(error);
            }
            transportInstanceStatusData.setWorkerId(assignedWorkerId);

            return transportInstanceStatusData;
        }

        @Override
        public TransportStatus getStatus(final String tenant,
                final String namespace,
                final String name,
                final Collection<Assignment> assignments,
                final URI uri) throws PulsarAdminException {
            TransportStatus transportStatus = new TransportStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData;
                if (isOwner) {
                    transportInstanceStatusData = getComponentInstanceStatus(tenant,
                            namespace, name, assignment.getInstance().getInstanceId(), null);
                } else {
                    transportInstanceStatusData = worker().getFunctionAdmin().transports().getTransportStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                TransportStatus.TransportInstanceStatus instanceStatus = new TransportStatus.TransportInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(transportInstanceStatusData);
                transportStatus.addInstance(instanceStatus);
            }

            transportStatus.setNumInstances(transportStatus.instances.size());
            transportStatus.getInstances().forEach(transportInstanceStatus -> {
                if (transportInstanceStatus.getStatus().isRunning()) {
                    transportStatus.numRunning++;
                }
            });
            return transportStatus;
        }

        @Override
        public TransportStatus getStatusExternal(final String tenant,
                final String namespace,
                final String name,
                final int parallelism) {
            TransportStatus transportStatus = new TransportStatus();
            for (int i = 0; i < parallelism; ++i) {
                TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                TransportStatus.TransportInstanceStatus transportInstanceStatus
                        = new TransportStatus.TransportInstanceStatus();
                transportInstanceStatus.setInstanceId(i);
                transportInstanceStatus.setStatus(transportInstanceStatusData);
                transportStatus.addInstance(transportInstanceStatus);
            }

            transportStatus.setNumInstances(transportStatus.instances.size());
            transportStatus.getInstances().forEach(sinkInstanceStatus -> {
                if (sinkInstanceStatus.getStatus().isRunning()) {
                    transportStatus.numRunning++;
                }
            });
            return transportStatus;
        }

        @Override
        public TransportStatus emptyStatus(final int parallelism) {
            TransportStatus transportStatus = new TransportStatus();
            transportStatus.setNumInstances(parallelism);
            transportStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                TransportStatus.TransportInstanceStatus transportInstanceStatus = new TransportStatus.TransportInstanceStatus();
                transportInstanceStatus.setInstanceId(i);
                TransportStatus.TransportInstanceStatus.TransportInstanceStatusData transportInstanceStatusData
                        = new TransportStatus.TransportInstanceStatus.TransportInstanceStatusData();
                transportInstanceStatusData.setRunning(false);
                transportInstanceStatusData.setError("Transport has not been scheduled");
                transportInstanceStatus.setStatus(transportInstanceStatusData);

                transportStatus.addInstance(transportInstanceStatus);
            }

            return transportStatus;
        }
    }

    private ExceptionInformation getExceptionInformation(InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation
                = new ExceptionInformation();
        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
        return exceptionInformation;
    }

    public List<ConnectorDefinition> getTransportList() {
        List<ConnectorDefinition> connectorDefinitions = getListOfConnectors();
        List<ConnectorDefinition> retval = new ArrayList<>();
        for (ConnectorDefinition connectorDefinition : connectorDefinitions) {
            if (!isEmpty(connectorDefinition.getSinkClass()) && !isEmpty(connectorDefinition.getSourceClass())) {
                retval.add(connectorDefinition);
            }
        }
        return retval;
    }

    public List<ConfigFieldDefinition> getTransportConfigDefinition(String name) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        List<ConfigFieldDefinition> retval = this.worker().getConnectorsManager().getTransportConfigDefinition(name);
        if (retval == null) {
            throw new RestException(Response.Status.NOT_FOUND, "builtin sink does not exist");
        }
        return retval;
    }
}
