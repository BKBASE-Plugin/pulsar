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
package org.apache.pulsar.common.io;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.WindowConfig;


/**
 * Pulsar Transport configuration.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TransportConfig {
    //common
    private String runtimeFlags;
    private String tenant;
    private String namespace;
    private String name;
    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    private FunctionConfig.Runtime runtime;
    private String customRuntimeOptions;
    private Resources resources;
    private Integer parallelism;
    private String archive;
    private Boolean autoAck;
    private Boolean retainOrdering;

    // source sink
    private String sinkClassName;
    private String sourceClassName;
    private Map<String, Object> configs;

    // functions
    private String functionClassName;
    private String logTopic;
    private Map<String, Object> userConfig;
    private Map<String, Object> secrets;
    private WindowConfig windowConfig;

}
