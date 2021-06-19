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
    private Integer maxPendingAsyncRequests;

    // source sink
    private String sinkClassName;
    private String sourceClassName;
    private Map<String, Object> configs;

    // functions
    private String functionClassName;
    private String logTopic;
    private Map<String, Object> userConfig;
    private Map<String, Object> secrets;
    private String fqfn;
    private WindowConfig windowConfig;
    private String jar;
    private String py;
    private String go;


}
