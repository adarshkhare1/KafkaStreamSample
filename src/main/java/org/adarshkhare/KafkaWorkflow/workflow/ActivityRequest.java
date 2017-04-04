package org.adarshkhare.KafkaWorkflow.workflow;

import java.util.UUID;

public class ActivityRequest
{


    private final String parentTaskId;
    private final String activityId;
    private String payload;

    public ActivityRequest(String taskId)
    {
        this.parentTaskId = taskId;
        this.activityId = UUID.randomUUID().toString();
    }

    public String getParentTaskId()
    {
        return parentTaskId;
    }

    public String getActivityId()
    {
        return activityId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
