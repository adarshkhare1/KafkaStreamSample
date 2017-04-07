package org.adarshkhare.KafkaWorkflow.activity;


public abstract class BaseWorkflowActivity
{
    protected final String activityName;

    protected  BaseWorkflowActivity(String name)
    {
        this.activityName = name;
    }

    public abstract void onReceive(Object message);

}
