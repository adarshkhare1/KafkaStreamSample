package org.adarshkhare.KafkaWorkflow.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import akka.actor.*;
import org.adarshkhare.KafkaWorkflow.activity.TestActivity1;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import org.adarshkhare.KafkaWorkflow.workflow.WorkflowStartRequest;
import org.adarshkhare.KafkaWorkflow.workflow.WorkflowStartResponse;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

public class WorkflowMessageRouter extends UntypedActor
{
    /**
     *
     */
    public static final ActorSystem MessageRoutingSystem;
    public static final FiniteDuration DefaultActorTimeout;
    private static final Inbox inbox;
    private static final Logger _LOGGER;
    private static final Map<String, ActorRef> activityActors;
    /*
         * Initialize the static members
         */
    static
    {
        DefaultActorTimeout = Duration.create(15, TimeUnit.SECONDS);
        _LOGGER = Logger.getLogger(WorkflowMessageRouter.class.getName());
        MessageRoutingSystem = ActorSystem.create("WorkflowActors");
        inbox = Inbox.create(WorkflowMessageRouter.MessageRoutingSystem);
        activityActors = new HashMap<String, ActorRef>();
    }

    @Override
    public void onReceive(Object message) throws Throwable
    {
        _LOGGER.entering(WorkflowMessageRouter.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        Object result = null;
        try
        {
            if (message instanceof ActivityRequest)
            {
                ActivityRequest req = (ActivityRequest) message;
                _LOGGER.log(Level.INFO, "Finding the actor for "+req.getActivityId());
                ActorRef activity = this.getActivityActor(req);
                if (activity != null)
                {
                    inbox.send(activity, message);
                    result = inbox.receive(WorkflowMessageRouter.DefaultActorTimeout);
                    getSender().tell(result, getSelf());
                }
            }
            if(result == null)
            {
                unhandled(message);
                _LOGGER.log(Level.WARNING, "Got a null reply from an actor");
            }
        }
        catch (Exception ex)
        {
            unhandled(message);
            _LOGGER.log(Level.WARNING, "Got a timeout waiting for reply from an actor:" + ex.getMessage());
        }
        _LOGGER.exiting(WorkflowMessageRouter.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
    }

    private synchronized ActorRef getActivityActor(ActivityRequest req)
    {
        ActorRef activityActor = null;
        String TaskId = req.getParentTaskId();
        String activityId = req.getActivityId();
        if (activityId != null && TaskId != null)
        {
            String actorKey = TaskId+":"+activityId;
            if (activityActors.containsKey(actorKey))
            {
                _LOGGER.log(Level.INFO, String.format("Found actor for %s", actorKey));
                activityActor = activityActors.get(actorKey);
            }
            else
            {
                activityActor = instantiateNewActivityActor(TaskId, activityId);
                if (activityActor != null)
                {
                    activityActors.put(actorKey, activityActor);
                }
            }
        }
        return activityActor;
    }

    private ActorRef instantiateNewActivityActor(String taskId, String activityId)
    {
        ActorRef actor = null;
        if (activityId.equalsIgnoreCase(TestActivity1.TestActivity1Name))
        {
            actor = MessageRoutingSystem.actorOf(Props.create(TestActivity1.class), activityId);
        }
        return actor;
    }
}
