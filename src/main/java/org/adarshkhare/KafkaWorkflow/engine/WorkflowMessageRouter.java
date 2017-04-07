package org.adarshkhare.KafkaWorkflow.engine;

import org.adarshkhare.KafkaWorkflow.activity.BaseWorkflowActivity;
import org.adarshkhare.KafkaWorkflow.activity.TestActivity1;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkflowMessageRouter
{
    /**
     *
     */
    public static final FiniteDuration DefaultActorTimeout;
    private static final Logger _LOGGER;
    private static final Map<String, BaseWorkflowActivity> activityMap;
    /*
         * Initialize the static members
         */
    static
    {
        DefaultActorTimeout = Duration.create(15, TimeUnit.SECONDS);
        _LOGGER = Logger.getLogger(WorkflowMessageRouter.class.getName());
        activityMap = new HashMap<String, BaseWorkflowActivity>();
    }

    public void RouteMessageToWorker(Object message)
    {
        _LOGGER.entering(WorkflowMessageRouter.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        Object result = null;
        try
        {
            if (message instanceof ActivityRequest)
            {
                ActivityRequest req = (ActivityRequest) message;
                _LOGGER.log(Level.INFO, "Finding the actor for "+req.getActivityId());
                BaseWorkflowActivity activity = this.getActivityActor(req);
                if (activity != null)
                {
                    activity.onReceive(message);
                }
            }
        }
        catch (Exception ex)
        {
            _LOGGER.log(Level.WARNING, "Exception when activity processing the message:" + ex.getMessage());
            throw ex;
        }
        _LOGGER.exiting(WorkflowMessageRouter.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
    }

    private synchronized BaseWorkflowActivity getActivityActor(ActivityRequest req)
    {
        BaseWorkflowActivity activity = null;
        String TaskId = req.getParentTaskId();
        String activityId = req.getActivityId();
        if (activityId != null && TaskId != null)
        {
            String actorKey = TaskId+":"+activityId;
            if (activityMap.containsKey(actorKey))
            {
                _LOGGER.log(Level.INFO, String.format("Found actor for %s", actorKey));
                activity = activityMap.get(actorKey);
            }
            else
            {
                activity = instantiateNewActivity(TaskId, activityId);
                if (activity != null)
                {
                    activityMap.put(actorKey, activity);
                }
            }
        }
        return activity;
    }

    private BaseWorkflowActivity instantiateNewActivity(String taskId, String activityId)
    {
        BaseWorkflowActivity activity = null;
        if (activityId.equalsIgnoreCase(TestActivity1.TestActivity1Name))
        {
            activity = new TestActivity1();
        }
        return activity;
    }
}
