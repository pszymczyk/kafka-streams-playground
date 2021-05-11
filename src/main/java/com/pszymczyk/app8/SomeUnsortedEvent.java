package com.pszymczyk.app8;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SomeUnsortedEvent {
    private String processId;
    private Integer order;

    @JsonCreator
    public SomeUnsortedEvent(
        @JsonProperty("processId") String processId,
        @JsonProperty("order") Integer order) {
        this.processId = processId;
        this.order = order;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }
}
