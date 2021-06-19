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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.TransportConfig;
import org.apache.pulsar.common.policies.data.TransportStatus;


/**
 * Admin interface for Transport management.
 */
public interface Transports {
    /**
     * Get the list of Transports.
     * <p/>
     * Get the list of all the Pulsar Transports.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> listTransports(String tenant, String namespace) throws PulsarAdminException;

    /**
     * Get the list of Transports asynchronously.
     * <p/>
     * Get the list of all the Pulsar Transports.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     */
    CompletableFuture<List<String>> listTransportsAsync(String tenant, String namespace);

    /**
     * Get the configuration for the specified Transport.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @return the Transport configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    TransportConfig getTransport(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Get the configuration for the specified Transport asynchronously.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @return the Transport configuration
     */
    CompletableFuture<TransportConfig> getTransportAsync(String tenant, String namespace, String transport);

    /**
     * Create a new Transport.
     *
     * @param transportConfig
     *            the Transport configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createTransport(TransportConfig transportConfig, String fileName) throws PulsarAdminException;

    /**
     * Create a new Transport asynchronously.
     *
     * @param transportConfig
     *            the Transport configuration object
     */
    CompletableFuture<Void> createTransportAsync(TransportConfig transportConfig, String fileName);

    /**
     * Create a new Transport with package url.
     * <p/>
     * Create a new Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createTransportWithUrl(TransportConfig transportConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Create a new Transport with package url asynchronously.
     * <p/>
     * Create a new Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> createTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl);

    /**
     * Update the configuration for a Transport.
     * <p/>
     *
     * @param transportConfig
     *            the Transport configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateTransport(TransportConfig transportConfig, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a Transport asynchronously.
     * <p/>
     *
     * @param transportConfig
     *            the Transport configuration object
     */
    CompletableFuture<Void> updateTransportAsync(TransportConfig transportConfig, String fileName);

    /**
     * Update the configuration for a Transport.
     * <p/>
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateTransport(TransportConfig transportConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a Transport asynchronously.
     * <p/>
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateTransportAsync(TransportConfig transportConfig, String fileName,
            UpdateOptions updateOptions);

    /**
     * Update the configuration for a Transport.
     * <p/>
     * Update a Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateTransportWithUrl(TransportConfig transportConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a Transport asynchronously.
     * <p/>
     * Update a Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> updateTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl);

    /**
     * Update the configuration for a Transport.
     * <p/>
     * Update a Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateTransportWithUrl(TransportConfig transportConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a Transport asynchronously.
     * <p/>
     * Update a Transport by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param transportConfig
     *            the Transport configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateTransportWithUrlAsync(TransportConfig transportConfig, String pkgUrl,
            UpdateOptions updateOptions);

    /**
     * Delete an existing Transport.
     * <p/>
     * Delete a Transport
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Cluster does not exist
     * @throws PreconditionFailedException
     *             Cluster is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteTransport(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Delete an existing Transport asynchronously.
     * <p/>
     * Delete a Transport
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     */
    CompletableFuture<Void> deleteTransportAsync(String tenant, String namespace, String transport);

    /**
     * Gets the current status of a Transport.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    TransportStatus getTransportStatus(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Gets the current status of a Transport asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     */
    CompletableFuture<TransportStatus> getTransportStatusAsync(String tenant, String namespace, String transport);

    /**
     * Gets the current status of a Transport instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param id
     *            Transport instance-id
     * @return
     * @throws PulsarAdminException
     */
    TransportStatus.TransportInstanceStatus.TransportInstanceStatusData getTransportStatus(
            String tenant, String namespace, String transport, int id)
            throws PulsarAdminException;

    /**
     * Gets the current status of a Transport instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param id
     *            Transport instance-id
     * @return
     */
    CompletableFuture<TransportStatus.TransportInstanceStatus.TransportInstanceStatusData> getTransportStatusAsync(
            String tenant, String namespace, String transport, int id);

    /**
     * Restart Transport instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartTransport(String tenant, String namespace, String transport, int instanceId)
            throws PulsarAdminException;

    /**
     * Restart Transport instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     */
    CompletableFuture<Void> restartTransportAsync(String tenant, String namespace, String transport, int instanceId);

    /**
     * Restart all Transport instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartTransport(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Restart all Transport instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     */
    CompletableFuture<Void> restartTransportAsync(String tenant, String namespace, String transport);

    /**
     * Stop Transport instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopTransport(String tenant, String namespace, String transport, int instanceId) throws PulsarAdminException;

    /**
     * Stop Transport instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     */
    CompletableFuture<Void> stopTransportAsync(String tenant, String namespace, String transport, int instanceId);

    /**
     * Stop all Transport instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopTransport(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Stop all Transport instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     */
    CompletableFuture<Void> stopTransportAsync(String tenant, String namespace, String transport);

    /**
     * Start Transport instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startTransport(String tenant, String namespace, String transport, int instanceId) throws PulsarAdminException;

    /**
     * Start Transport instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     * @param instanceId
     *            Transport instanceId
     */
    CompletableFuture<Void> startTransportAsync(String tenant, String namespace, String transport, int instanceId);

    /**
     * Start all Transport instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startTransport(String tenant, String namespace, String transport) throws PulsarAdminException;

    /**
     * Start all Transport instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param transport
     *            Transport name
     */
    CompletableFuture<Void> startTransportAsync(String tenant, String namespace, String transport);

    /**
     * Fetches a list of supported Pulsar IO Transports currently running in cluster mode.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<ConnectorDefinition> getBuiltInTransports() throws PulsarAdminException;

    /**
     * Fetches a list of supported Pulsar IO Transports currently running in cluster mode asynchronously.
     */
    CompletableFuture<List<ConnectorDefinition>> getBuiltInTransportsAsync();

    /**
     * Reload the available built-in connectors, include Source and Transport.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void reloadBuiltInTransports() throws PulsarAdminException;

    /**
     * Reload the available built-in connectors, include Source and Transport asynchronously.
     */
    CompletableFuture<Void> reloadBuiltInTransportsAsync();
}
