package org.apache.pulsar.functions.worker.rest.api.v3;


import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.policies.data.TransportStatus;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.apache.pulsar.functions.worker.rest.api.TransportsImpl;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Slf4j
@Api(value = "/transports", description = "Transports admin apis", tags = "transports")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/transports")
public class TransportsApiV3Resource extends FunctionApiResource {
    protected final TransportsImpl transport;

    public TransportsApiV3Resource() {
        this.transport = new TransportsImpl(this);
    }

    @POST
    @Path("/{tenant}/{namespace}/{transportName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            final @FormDataParam("transportConfig") TransportConfig transportConfig) {

        transport.registerTransport(tenant, namespace, transportName, uploadedInputStream, fileDetail,
                functionPkgUrl, transportConfig, clientAppId(), clientAuthData());
    }

    @PUT
    @Path("/{tenant}/{namespace}/{transportName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String functionPkgUrl,
            final @FormDataParam("transportConfig") TransportConfig transportConfig,
            final @FormDataParam("updateOptions") UpdateOptions updateOptions) {

        transport.updateTransport(tenant, namespace, transportName, uploadedInputStream, fileDetail,
                functionPkgUrl, transportConfig, clientAppId(), clientAuthData(), updateOptions);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{transportName}")
    public void deregisterTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName) {
        transport.deregisterFunction(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @GET
    @Path("/{tenant}/{namespace}/{transportName}")
    public TransportConfig getTransportInfo(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName)
            throws IOException {
        return transport.getTransportInfo(tenant, namespace, transportName);
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Transport instance",
            response = TransportStatus.TransportInstanceStatus.TransportInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Transport doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/status")
    public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData getTransportInstanceStatus(
            final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @PathParam("instanceId") String instanceId) throws IOException {
        return transport.getTransportInstanceStatus(tenant, namespace, transportName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Transport running in cluster mode",
            response = TransportStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "The Transport doesn't exist")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{transportName}/status")
    public TransportStatus getTransportStatus(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName) throws IOException {
        return transport.getTransportStatus(tenant, namespace, transportName, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @Path("/{tenant}/{namespace}")
    public List<String> listTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace) {
        return transport.listFunctions(tenant, namespace, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart Transport instance", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @PathParam("instanceId") String instanceId) {
        transport.restartFunctionInstance(tenant, namespace, transportName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart all Transport instances", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"), @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName) {
        transport.restartFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop Transport instance", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @PathParam("instanceId") String instanceId) {
        transport.stopFunctionInstance(tenant, namespace, transportName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop all Transport instances", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName) {
        transport.stopFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start Transport instance", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName,
            final @PathParam("instanceId") String instanceId) {
        transport.startFunctionInstance(tenant, namespace, transportName, instanceId, this.uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start all Transport instances", response = Void.class)
    @ApiResponses(value = {@ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The function does not exist"),
            @ApiResponse(code = 500, message = "Internal server error")})
    @Path("/{tenant}/{namespace}/{transportName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startTransport(final @PathParam("tenant") String tenant,
            final @PathParam("namespace") String namespace,
            final @PathParam("transportName") String transportName) {
        transport.startFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("/builtintransports")
    public List<ConnectorDefinition> getTransportList() {
        return transport.getTransportList();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about config fields associated with the specified builtin sink",
            response = ConfigFieldDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "builtin sink does not exist"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtintransports/{name}/configdefinition")
    public List<ConfigFieldDefinition> getTransportConfigDefinition(
            @ApiParam(value = "The name of the builtin transport")
            final @PathParam("name") String name) throws IOException {
        return transport.getTransportConfigDefinition(name);
    }


    @POST
    @ApiOperation(
            value = "Reload the built-in connectors, including Sources and Transports",
            response = Void.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/reloadBuiltInTransports")
    public void reloadTransports() {
        transport.reloadConnectors(clientAppId());
    }
}
