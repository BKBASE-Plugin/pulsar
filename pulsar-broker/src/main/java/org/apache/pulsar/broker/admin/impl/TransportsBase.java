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
package org.apache.pulsar.broker.admin.impl;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
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
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.policies.data.TransportStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Transports;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

public class TransportsBase extends AdminResource {

    Transports<? extends WorkerService> transports() {
        return pulsar().getWorkerService().getTransports();
    }

    @POST
    @ApiOperation(value = "Creates a new Pulsar Transport in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request (The Pulsar Transport already exists, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Transport successfully created"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize,"
                            + " failed to get tenant data, failed to process package, etc.)"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerTransport(
            @ApiParam(value = "The tenant of a Pulsar Transport") final @PathParam("tenant") String tenant,
                             @ApiParam(value = "The namespace of a Pulsar Transport") final @PathParam("namespace")
                                     String namespace,
                             @ApiParam(value = "The name of a Pulsar Transport") final @PathParam("transportName")
                                         String transportName,
                             final @FormDataParam("data") InputStream uploadedInputStream,
                             final @FormDataParam("data") FormDataContentDisposition fileDetail,
                             final @FormDataParam("url") String transportPkgUrl,
                             @ApiParam(value =
                                     "A JSON value presenting config payload of a Pulsar Transport."
                                             + " All available configuration options are:\n"
                                             + "- **classname**\n"
                                             + "   The class name of a Pulsar Transport if"
                                             + " archive is file-url-path (file://)\n"
                                             + "- **sourceSubscriptionName**\n"
                                             + "   Pulsar source subscription name if"
                                             + " user wants a specific\n"
                                             + "   subscription-name for input-topic consumer\n"
                                             + "- **inputs**\n"
                                             + "   The input topic or topics of"
                                             + " a Pulsar Transport (specified as a JSON array)\n"
                                             + "- **topicsPattern**\n"
                                             + "   TopicsPattern to consume from list of topics under a namespace that "
                                             + "   match the pattern. [input] and [topicsPattern] are mutually "
                                             + "   exclusive. Add SerDe class name for a pattern in customSerdeInputs "
                                             + "   (supported for java fun only)"
                                             + "- **topicToSerdeClassName**\n"
                                             + "   The map of input topics to SerDe class names"
                                             + " (specified as a JSON object)\n"
                                             + "- **topicToSchemaType**\n"
                                             + "   The map of input topics to Schema types or class names"
                                             + " (specified as a JSON object)\n"
                                             + "- **inputSpecs**\n"
                                             + "   The map of input topics to its consumer configuration,"
                                             + " each configuration has schema of "
                                             + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                                             + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                                             + "- **configs**\n"
                                             + "   The map of configs (specified as a JSON object)\n"
                                             + "- **secrets**\n"
                                             + "   a map of secretName(aka how the secret is going to be \n"
                                             + "   accessed in the function via context) to an object that \n"
                                             + "   encapsulates how the secret is fetched by the underlying \n"
                                             + "   secrets provider. The type of an value here can be found by the \n"
                                             + "   SecretProviderConfigurator.getSecretObjectType() method."
                                             + " (specified as a JSON object)\n"
                                             + "- **parallelism**\n"
                                             + "   The parallelism factor of a Pulsar Transport"
                                             + " (i.e. the number of a Pulsar Transport instances to run \n"
                                             + "- **processingGuarantees**\n"
                                             + "   The processing guarantees (aka delivery semantics) applied to"
                                             + " the Pulsar Transport. Possible Values: \"ATLEAST_ONCE\","
                                             + " \"ATMOST_ONCE\", \"EFFECTIVELY_ONCE\"\n"
                                             + "- **retainOrdering**\n"
                                             + "   Boolean denotes whether the Pulsar Transport"
                                             + " consumes and processes messages in order\n"
                                             + "- **resources**\n"
                                             + "   {\"cpu\": 1, \"ram\": 2, \"disk\": 3} The CPU (in cores),"
                                             + " RAM (in bytes) and disk (in bytes) that needs to be "
                                             + "allocated per Pulsar Transport instance "
                                             + "(applicable only to Docker runtime)\n"
                                             + "- **autoAck**\n"
                                             + "   Boolean denotes whether or not the framework"
                                             + " will automatically acknowledge messages\n"
                                             + "- **timeoutMs**\n"
                                             + "   Long denotes the message timeout in milliseconds\n"
                                             + "- **cleanupSubscription**\n"
                                             + "   Boolean denotes whether the subscriptions the functions"
                                             + " created/used should be deleted when the functions is deleted\n"
                                             + "- **runtimeFlags**\n"
                                             + "   Any flags that you want to pass to the runtime as a single string\n",
                                     examples = @Example(
                                             value = @ExampleProperty(
                                                     mediaType = MediaType.APPLICATION_JSON,
                                                     value = "{\n"
                                                             + "\t\"classname\": \"org.example.MyTransportTest\",\n"
                                                             + "\t\"inputs\": ["
                                                             + "\"persistent://public/default/Transport-input\"],\n"
                                                             + "\t\"processingGuarantees\": \"EFFECTIVELY_ONCE\",\n"
                                                             + "\t\"parallelism\": 10\n"
                                                             + "}"
                                             )
                                 )
                             )
                             final @FormDataParam("transportConfig") TransportConfig transportConfig) {
        transports().registerTransport(tenant, namespace, transportName, uploadedInputStream, fileDetail,
                transportPkgUrl, transportConfig, clientAppId(), clientAuthData());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Transport currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message =
                    "Invalid request (The Pulsar Transport doesn't exist, update contains no change, etc.)"),
            @ApiResponse(code = 200, message = "Pulsar Transport successfully updated"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "The Pulsar Transport doesn't exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize, failed to process package, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateTransport(
            @ApiParam(value = "The tenant of a Pulsar Transport") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Transport") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Transport") final @PathParam("transportName") String transportName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String transportPkgUrl,
            @ApiParam(value =
                                   "A JSON value presenting config payload of a Pulsar Transport."
                                           + " All available configuration options are:\n"
                                           + "- **classname**\n"
                                           + "   The class name of a Pulsar Transport if"
                                           + " archive is file-url-path (file://)\n"
                                           + "- **sourceSubscriptionName**\n"
                                           + "   Pulsar source subscription name if user wants a specific\n"
                                           + "   subscription-name for input-topic consumer\n"
                                           + "- **inputs**\n"
                                           + "   The input topic or topics of"
                                           + " a Pulsar Transport (specified as a JSON array)\n"
                                           + "- **topicsPattern**\n"
                                           + "   TopicsPattern to consume from list of topics under a namespace that "
                                           + "   match the pattern. [input] and [topicsPattern] are mutually "
                                           + "   exclusive. Add SerDe class name for a pattern in customSerdeInputs "
                                           + "   (supported for java fun only)"
                                           + "- **topicToSerdeClassName**\n"
                                           + "   The map of input topics to"
                                           + " SerDe class names (specified as a JSON object)\n"
                                           + "- **topicToSchemaType**\n"
                                           + "   The map of input topics to Schema types or"
                                           + " class names (specified as a JSON object)\n"
                                           + "- **inputSpecs**\n"
                                           + "   The map of input topics to its consumer configuration,"
                                           + " each configuration has schema of "
                                           + "   {\"schemaType\": \"type-x\", \"serdeClassName\": \"name-x\","
                                           + " \"isRegexPattern\": true, \"receiverQueueSize\": 5}\n"
                                           + "- **configs**\n"
                                           + "   The map of configs (specified as a JSON object)\n"
                                           + "- **secrets**\n"
                                           + "   a map of secretName(aka how the secret is going to be \n"
                                           + "   accessed in the function via context) to an object that \n"
                                           + "   encapsulates how the secret is fetched by the underlying \n"
                                           + "   secrets provider. The type of an value here can be found by the \n"
                                           + "   SecretProviderConfigurator.getSecretObjectType() method."
                                           + " (specified as a JSON object)\n"
                                           + "- **parallelism**\n"
                                           + "   The parallelism factor of a Pulsar Transport "
                                           + "(i.e. the number of a Pulsar Transport instances to run \n"
                                           + "- **processingGuarantees**\n"
                                           + "   The processing guarantees (aka delivery semantics) applied to the"
                                           + " Pulsar Transport. Possible Values: \"ATLEAST_ONCE\", \"ATMOST_ONCE\","
                                           + " \"EFFECTIVELY_ONCE\"\n"
                                           + "- **retainOrdering**\n"
                                           + "   Boolean denotes whether the Pulsar Transport"
                                           + " consumes and processes messages in order\n"
                                           + "- **resources**\n"
                                           + "   {\"cpu\": 1, \"ram\": 2, \"disk\": 3} The CPU (in cores),"
                                           + " RAM (in bytes) and disk (in bytes) that needs to be allocated per"
                                           + " Pulsar Transport instance (applicable only to Docker runtime)\n"
                                           + "- **autoAck**\n"
                                           + "   Boolean denotes whether or not the framework will"
                                           + " automatically acknowledge messages\n"
                                           + "- **timeoutMs**\n"
                                           + "   Long denotes the message timeout in milliseconds\n"
                                           + "- **cleanupSubscription**\n"
                                           + "   Boolean denotes whether the subscriptions the functions"
                                           + " created/used should be deleted when the functions is deleted\n"
                                           + "- **runtimeFlags**\n"
                                           + "   Any flags that you want to pass to the runtime as a single string\n",
                                   examples = @Example(
                                           value = @ExampleProperty(
                                                   mediaType = MediaType.APPLICATION_JSON,
                                                   value = "{\n"
                                                           + "\t\"classname\": \"org.example.TransportStressTest\",\n"
                                                           + "\t\"inputs\": ["
                                                           + "\"persistent://public/default/Transport-input\"],\n"
                                                           + "\t\"processingGuarantees\": \"EFFECTIVELY_ONCE\",\n"
                                                           + "\t\"parallelism\": 5\n"
                                                           + "}"
                                           )
                               )
                           )
                           final @FormDataParam("transportConfig") TransportConfig transportConfig,
                           @ApiParam(value = "Update options for the Pulsar Transport")
                           final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {
         transports().updateTransport(tenant, namespace, transportName, uploadedInputStream, fileDetail,
                transportPkgUrl, transportConfig, clientAppId(), clientAuthData(), updateOptions);

    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Transport currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid deregister request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 200, message = "The Pulsar Transport was successfully deleted"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to authorize, failed to deregister, etc.)"),
            @ApiResponse(code = 408, message = "Got InterruptedException while deregistering the Pulsar Transport"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}")
    public void deregisterTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                               final @PathParam("tenant") String tenant,
                               @ApiParam(value = "The namespace of a Pulsar Transport")
                               final @PathParam("namespace") String namespace,
                               @ApiParam(value = "The name of a Pulsar Transport")
                               final @PathParam("transportName") String transportName) {
        transports().deregisterFunction(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about a Pulsar Transport currently running in cluster mode",
            response = TransportConfig.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}")
    public TransportConfig getTransportInfo(@ApiParam(value = "The tenant of a Pulsar Transport")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The namespace of a Pulsar Transport")
                                  final @PathParam("namespace") String namespace,
                                  @ApiParam(value = "The name of a Pulsar Transport")
                                  final @PathParam("transportName") String transportName) throws IOException {
        return transports().getTransportInfo(tenant, namespace, transportName);
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Transport instance",
            response = TransportStatus.TransportInstanceStatus.TransportInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "The Pulsar Transport instance does not exist"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message = "Internal Server Error (got exception while getting status, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/status")
    public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData getTransportInstanceStatus(
            @ApiParam(value = "The tenant of a Pulsar Transport")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Transport")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Transport")
            final @PathParam("transportName") String transportName,
            @ApiParam(value = "The instanceId of a Pulsar Transport")
            final @PathParam("instanceId") String instanceId) throws IOException {
        return transports().getTransportInstanceStatus(
            tenant, namespace, transportName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Transport running in cluster mode",
            response = TransportStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "Invalid get status request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{transportName}/status")
    public TransportStatus getTransportStatus(@ApiParam(value = "The tenant of a Pulsar Transport")
                                    final @PathParam("tenant") String tenant,
                                    @ApiParam(value = "The namespace of a Pulsar Transport")
                                    final @PathParam("namespace") String namespace,
                                    @ApiParam(value = "The name of a Pulsar Transport")
                                    final @PathParam("transportName") String transportName) throws IOException {
        return transports().getTransportStatus(tenant, namespace, transportName, uri.getRequestUri(), clientAppId(),
                clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Lists all Pulsar Transports currently deployed in a given namespace",
            response = String.class,
            responseContainer = "Collection"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid list request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 500, message = "Internal server error (failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}")
    public List<String> listTransports(@ApiParam(value = "The tenant of a Pulsar Transport")
                                  final @PathParam("tenant") String tenant,
                                  @ApiParam(value = "The namespace of a Pulsar Transport")
                                  final @PathParam("namespace") String namespace) {
        return transports().listFunctions(tenant, namespace, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart an instance of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this Transport"),
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to restart the instance of"
                            + " a Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The namespace of a Pulsar Transport")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The name of a Pulsar Transport")
                            final @PathParam("transportName") String transportName,
                            @ApiParam(value = "The instanceId of a Pulsar Transport")
                            final @PathParam("instanceId") String instanceId) {
        transports().restartFunctionInstance(tenant, namespace, transportName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Restart all instances of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid restart request"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to restart the Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                            final @PathParam("tenant") String tenant,
                            @ApiParam(value = "The namespace of a Pulsar Transport")
                            final @PathParam("namespace") String namespace,
                            @ApiParam(value = "The name of a Pulsar Transport")
                            final @PathParam("transportName") String transportName) {
        transports().restartFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop an instance of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport instance does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to stop the Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The namespace of a Pulsar Transport")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The name of a Pulsar Transport")
                         final @PathParam("transportName") String transportName,
                         @ApiParam(value = "The instanceId of a Pulsar Transport")
                         final @PathParam("instanceId") String instanceId) {
        transports().stopFunctionInstance(tenant, namespace,
                transportName, instanceId, uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Stop all instances of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid stop request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to stop the Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                         final @PathParam("tenant") String tenant,
                         @ApiParam(value = "The namespace of a Pulsar Transport")
                         final @PathParam("namespace") String namespace,
                         @ApiParam(value = "The name of a Pulsar Transport")
                         final @PathParam("transportName") String transportName) {
        transports().stopFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start an instance of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to start the Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The namespace of a Pulsar Transport")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The name of a Pulsar Transport")
                          final @PathParam("transportName") String transportName,
                          @ApiParam(value = "The instanceId of a Pulsar Transport")
                          final @PathParam("instanceId") String instanceId) {
        transports().startFunctionInstance(tenant, namespace, transportName, instanceId,
                uri.getRequestUri(), clientAppId(), clientAuthData());
    }

    @POST
    @ApiOperation(value = "Start all instances of a Pulsar Transport", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid start request"),
            @ApiResponse(code = 404, message = "The Pulsar Transport does not exist"),
            @ApiResponse(code = 500, message =
                    "Internal server error (failed to start the Pulsar Transport, failed to authorize, etc.)"),
            @ApiResponse(code = 401, message = "The client is not authorized to perform this operation"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{transportName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startTransport(@ApiParam(value = "The tenant of a Pulsar Transport")
                          final @PathParam("tenant") String tenant,
                          @ApiParam(value = "The namespace of a Pulsar Transport")
                          final @PathParam("namespace") String namespace,
                          @ApiParam(value = "The name of a Pulsar Transport")
                          final @PathParam("transportName") String transportName) {
        transports().startFunctionInstances(tenant, namespace, transportName, clientAppId(), clientAuthData());
    }

    @GET
    @ApiOperation(
            value = "Fetches the list of built-in Pulsar IO Transports",
            response = ConnectorDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Get builtin Transports successfully.")
    })
    @Path("/builtintransports")
    public List<ConnectorDefinition> getTransportList() {
        return transports().getTransportList();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about config fields associated with the specified builtin Transport",
            response = ConfigFieldDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "builtin Transport does not exist"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtintransports/{name}/configdefinition")
    public List<ConfigFieldDefinition> gettransportConfigDefinition(
            @ApiParam(value = "The name of the builtin Transport")
            final @PathParam("name") String name) throws IOException {
        return transports().getTransportConfigDefinition(name);
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
        transports().reloadConnectors(clientAppId(), clientAuthData());
    }
}
