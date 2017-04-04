package org.adarshkhare.KafkaWorkflow.activity;


import akka.actor.UntypedActor;

public abstract class BaseWorkflowActivity extends UntypedActor
{
    protected final String activityName;

    protected  BaseWorkflowActivity(String name)
    {
        this.activityName = name;
    }

}
