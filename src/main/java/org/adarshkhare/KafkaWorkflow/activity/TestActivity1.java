package org.adarshkhare.KafkaWorkflow.activity;

import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityResponce;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TestActivity1 extends BaseWorkflowActivity
{
    public static final String TestActivity1Name = "TestActivity1_Name";

    private static final Logger _LOGGER;

    /*
	 * Initialize the static members
	 */
    static
    {
        _LOGGER = Logger.getLogger(TestActivity1.class.getName());
    }


    protected TestActivity1()
    {
        super(TestActivity1Name);
    }

    @Override
    public void onReceive(Object message) throws Throwable
    {
        Boolean isHandled = false;
        if (message instanceof ActivityRequest)
        {
            ActivityRequest request = (ActivityRequest) message;
            if (request.getActivityId().equalsIgnoreCase(TestActivity1Name))
            {
                ActivityResponce activityResponse = new ActivityResponce();
                _LOGGER.log(Level.INFO, "Completed the work for %s", TestActivity1Name);
                getSender().tell(activityResponse, getSelf());
                isHandled = true;
            }
        }
        if(!isHandled)
        {
            unhandled(message);
        }
    }
}
