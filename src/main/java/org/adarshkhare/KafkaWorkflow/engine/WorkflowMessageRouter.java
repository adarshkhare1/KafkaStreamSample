package org.adarshkhare.KafkaWorkflow.engine;

import org.adarshkhare.KafkaWorkflow.activity.BaseWorkflowActivity;
import org.adarshkhare.KafkaWorkflow.activity.TestActivity1;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        _LOGGER = LogManager.getLogger(WorkflowMessageRouter.class.getName());
        activityMap = new HashMap<String, BaseWorkflowActivity>();
    }

    public void RouteMessageToWorker(Object message)
    {
        _LOGGER.traceEntry();
        Object result = null;
        try
        {
            if (message instanceof ActivityRequest)
            {
                ActivityRequest req = (ActivityRequest) message;
                _LOGGER.info("Finding the actor for "+req.getActivityId());
                BaseWorkflowActivity activity = this.getActivityActor(req);
                if (activity != null)
                {
                    activity.onReceive(message);
                }
            }
        }
        catch (Exception ex)
        {
            _LOGGER.warn("Exception when activity processing the message:" + ex.getMessage());
            throw ex;
        }
        _LOGGER.traceExit();
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
                _LOGGER.info(String.format("Found actor for %s", actorKey));
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
