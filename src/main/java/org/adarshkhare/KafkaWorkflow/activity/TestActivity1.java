package org.adarshkhare.KafkaWorkflow.activity;

import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;

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


    public TestActivity1()
    {
        super(TestActivity1Name);
    }

    @Override
    public void onReceive(Object message)
    {
        if (message instanceof ActivityRequest)
        {
            ActivityRequest request = (ActivityRequest) message;
            if (request.getActivityId().equalsIgnoreCase(TestActivity1Name))
            {
                _LOGGER.log(Level.INFO, "Completed the work for %s", TestActivity1Name);
            }
        }
    }
}
