package org.adarshkhare.KafkaWorkflow.workflow;

public class WorkflowStartRequest
{
    private final String workFlowId;

    public WorkflowStartRequest(String id)
    {
        this.workFlowId = id;
    }

    public String getWorkFlowId()
    {
        return workFlowId;
    }

}
