package org.apache.pulsar.client.admin.internal;

import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;

import com.google.gson.Gson;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Transports;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.policies.data.TransportStatus;
import org.apache.pulsar.common.policies.data.TransportStatus.TransportInstanceStatus.TransportInstanceStatusData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;


@Slf4j
public class TransportsImpl extends ComponentResource implements Transports {


    private final WebTarget transports;
    private final AsyncHttpClient asyncHttpClient;

    public TransportsImpl(WebTarget web, Authentication auth, AsyncHttpClient asyncHttpClient, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.transports = web.path("/admin/v3/transports");
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public List<String> listTransports(String tenant, String namespace) throws PulsarAdminException {
        try {
            return listTransportsAsync(tenant, namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> listTransportsAsync(String tenant, String namespace) {
        WebTarget path = transports.path(tenant).path(namespace);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(new GenericType<List<String>>() {
                            }));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public TransportConfig getTransport(String tenant, String namespace, String transport) throws PulsarAdminException {
        try {
            return getTransportAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<TransportConfig> getTransportAsync(String tenant, String namespace, String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport);
        final CompletableFuture<TransportConfig> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(TransportConfig.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createTransport(TransportConfig transportConfig, String fileName) throws PulsarAdminException {
        try {
            createTransportAsync(transportConfig, fileName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createTransportAsync(TransportConfig transportConfig, String fileName) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    post(transports.path(transportConfig.getTenant())
                            .path(transportConfig.getNamespace()).path(transportConfig.getName()).getUri()
                            .toASCIIString())
                            .addBodyPart(new StringPart("transportConfig", ObjectMapperFactory.getThreadLocal()
                                    .writeValueAsString(transportConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpClient.executeRequest(addAuthHeaders(transports, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
                        } else {
                            future.complete(null);
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void createTransportWithUrl(TransportConfig transportConfig, String pkgUrl) throws PulsarAdminException {
        try {
            createTransportWithUrlAsync(transportConfig, pkgUrl).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl) {
        final FormDataMultiPart mp = new FormDataMultiPart();
        mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
        mp.bodyPart(new FormDataBodyPart("transportConfig",
                new Gson().toJson(transportConfig),
                MediaType.APPLICATION_JSON_TYPE));
        WebTarget path = transports.path(transportConfig.getTenant())
                .path(transportConfig.getNamespace())
                .path(transportConfig.getName());
        return asyncPostRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
    }

    @Override
    public void updateTransport(TransportConfig transportConfig, String fileName) throws PulsarAdminException {
        updateTransport(transportConfig, fileName, null);
    }

    @Override
    public CompletableFuture<Void> updateTransportAsync(TransportConfig transportConfig, String fileName) {
        return updateTransportAsync(transportConfig, fileName, null);
    }

    @Override
    public void updateTransport(TransportConfig transportConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException {
        try {
            updateTransportAsync(transportConfig, fileName, updateOptions)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateTransportAsync(TransportConfig transportConfig, String fileName,
            UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    put(transports.path(transportConfig.getTenant()).path(transportConfig.getNamespace())
                            .path(transportConfig.getName()).getUri().toASCIIString())
                            .addBodyPart(new StringPart("transportConfig", ObjectMapperFactory.getThreadLocal()
                                    .writeValueAsString(transportConfig), MediaType.APPLICATION_JSON));

            if (updateOptions != null) {
                builder.addBodyPart(new StringPart("updateOptions",
                        ObjectMapperFactory.getThreadLocal()
                                .writeValueAsString(updateOptions), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpClient.executeRequest(addAuthHeaders(transports, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
                        } else {
                            future.complete(null);
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateTransportWithUrl(TransportConfig transportConfig, String pkgUrl) throws PulsarAdminException {
        updateTransportWithUrl(transportConfig, pkgUrl, null);
    }

    @Override
    public CompletableFuture<Void> updateTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl) {
        return updateTransportWithUrlAsync(transportConfig, pkgUrl, null);
    }

    @Override
    public void updateTransportWithUrl(TransportConfig transportConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException {
        try {
            updateTransportWithUrlAsync(transportConfig, pkgUrl, updateOptions)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl,
            UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
            mp.bodyPart(new FormDataBodyPart(
                    "transportConfig",
                    new Gson().toJson(transportConfig),
                    MediaType.APPLICATION_JSON_TYPE));
            if (updateOptions != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            WebTarget path = transports.path(transportConfig.getTenant()).path(transportConfig.getNamespace())
                    .path(transportConfig.getName());
            return asyncPutRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void deleteTransport(String tenant, String namespace, String transport) throws PulsarAdminException {
        try {
            deleteTransportAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteTransportAsync(String tenant, String namespace, String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport);
        return asyncDeleteRequest(path);
    }

    @Override
    public TransportStatus getTransportStatus(String tenant, String namespace, String transport)
            throws PulsarAdminException {
        try {
            return getTransportStatusAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<TransportStatus> getTransportStatusAsync(String tenant, String namespace,
            String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path("status");
        final CompletableFuture<TransportStatus> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(TransportStatus.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public TransportInstanceStatusData getTransportStatus(String tenant, String namespace, String transport, int id)
            throws PulsarAdminException {
        try {
            return getTransportStatusAsync(tenant, namespace, transport, id)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<TransportInstanceStatusData> getTransportStatusAsync(String tenant, String namespace,
            String transport, int id) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path(Integer.toString(id))
                .path("status");
        final CompletableFuture<TransportInstanceStatusData> future =
                new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(
                                    TransportInstanceStatusData.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void restartTransport(String tenant, String namespace, String transport, int instanceId)
            throws PulsarAdminException {
        try {
            restartTransportAsync(tenant, namespace, transport, instanceId)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> restartTransportAsync(String tenant, String namespace, String transport,
            int instanceId) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path(Integer.toString(instanceId))
                .path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void restartTransport(String tenant, String namespace, String transport) throws PulsarAdminException {
        try {
            restartTransportAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> restartTransportAsync(String tenant, String namespace, String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopTransport(String tenant, String namespace, String transport, int instanceId)
            throws PulsarAdminException {
        try {
            stopTransportAsync(tenant, namespace, transport, instanceId).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> stopTransportAsync(String tenant, String namespace, String transport,
            int instanceId) {
        WebTarget path = transports.path(tenant)
                .path(namespace)
                .path(transport)
                .path(Integer.toString(instanceId))
                .path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopTransport(String tenant, String namespace, String transport) throws PulsarAdminException {
        try {
            stopTransportAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> stopTransportAsync(String tenant, String namespace, String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startTransport(String tenant, String namespace, String transport, int instanceId)
            throws PulsarAdminException {
        try {
            startTransportAsync(tenant, namespace, transport, instanceId)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startTransportAsync(String tenant, String namespace, String transport,
            int instanceId) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path(Integer.toString(instanceId))
                .path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startTransport(String tenant, String namespace, String transport) throws PulsarAdminException {
        try {
            startTransportAsync(tenant, namespace, transport).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startTransportAsync(String tenant, String namespace, String transport) {
        WebTarget path = transports.path(tenant).path(namespace).path(transport).path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<ConnectorDefinition> getBuiltInTransports() throws PulsarAdminException {
        try {
            return getBuiltInTransportsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<ConnectorDefinition>> getBuiltInTransportsAsync() {
        WebTarget path = transports.path("builtintransports");
        final CompletableFuture<List<ConnectorDefinition>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (!response.getStatusInfo().equals(Response.Status.OK)) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(
                                    new GenericType<List<ConnectorDefinition>>() {
                                    }));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void reloadBuiltInTransports() throws PulsarAdminException {
        try {
            reloadBuiltInTransportsAsync().get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> reloadBuiltInTransportsAsync() {
        WebTarget path = transports.path("reloadBuiltInTransports");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }
}
