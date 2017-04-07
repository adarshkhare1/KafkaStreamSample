package org.adarshkhare.KafkaWorkflow.workflow;

import java.util.UUID;

public class ActivityRequest
{
    private String parentTaskId;
    private String activityId;
    private String payload;

    public ActivityRequest()
    {

    }
    public ActivityRequest(String taskId)
    {
        this.parentTaskId = taskId;
        this.activityId = UUID.randomUUID().toString();
    }

    public String getParentTaskId()
    {
        return parentTaskId;
    }
    public void setParentTaskId(String parentTaskId) { this.parentTaskId = parentTaskId;}

    public String getActivityId()
    {
        return activityId;
    }
    public void setActivityId(String activityId) { this.activityId = activityId; }

    public String getPayload() {
        return payload;
    }
    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString()
    {
        return "parentTaskId=" + parentTaskId + ", activityId=" + activityId + ", payload=" + payload + "]";
    }
}
