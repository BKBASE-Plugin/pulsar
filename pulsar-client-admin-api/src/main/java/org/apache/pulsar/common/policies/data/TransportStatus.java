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
package org.apache.pulsar.common.policies.data;

import java.util.LinkedList;
import java.util.List;
import lombok.Data;


/**
 * Status of Pulsar Transport.
 */
@Data
public class TransportStatus {
    // The total number of Transport instances that ought to be running
    public int numInstances;
    // The number of source instances that are actually running
    public int numRunning;
    public List<TransportStatus.TransportInstanceStatus> instances = new LinkedList<>();

    /**
     * Status of a Transport instance.
     */
    @Data
    public static class TransportInstanceStatus {
        public int instanceId;
        public TransportStatus.TransportInstanceStatus.TransportInstanceStatusData status;

        /**
         * Status data of a Transport instance.
         */
        @Data
        public static class TransportInstanceStatusData {
            // Is this instance running?
            public boolean running;

            // Do we have any error while running this instance
            public String error;

            // Number of times this instance has restarted
            public long numRestarts;

            // Number of messages read from Pulsar
            public long numReadFromPulsar;

            // Number of messages received from source
            public long numReceivedFromSource;

            public long numReceived;

            public long numSuccessfullyProcessed;

            // Number of times there was a system exception handling messages
            public long numSystemExceptions;

            public long numUserExceptions;

            // A list of the most recent system exceptions
            public List<ExceptionInformation> latestSystemExceptions;

            // Number of times there was a Sink exception
            public long numSinkExceptions;

            // Number of times there was a exception from source while reading messages
            public long numSourceExceptions;

            // A list of the most recent Sink exceptions
            public List<ExceptionInformation> latestSinkExceptions;

            // A list of the most recent source exceptions
            public List<ExceptionInformation> latestSourceExceptions;

            public List<ExceptionInformation> latestUserExceptions;

            // Number of messages written to sink
            public long numWrittenToSink;

            // Number of messages written into pulsar
            public long numWritten;

            // When was the last time we received a message from Pulsar
            public long lastReceivedTime;

            public double averageLatency;

            public String workerId;
        }
    }

    public void addInstance(TransportStatus.TransportInstanceStatus transportInstanceStatus) {
        instances.add(transportInstanceStatus);
    }
}

