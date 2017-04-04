package org.adarshkhare.KafkaWorkflow.engine;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.adarshkhare.KafkaWorkflow.workflow.WorkflowStartRequest;
import org.adarshkhare.KafkaWorkflow.workflow.WorkflowStartResponse;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorSystem;
import akka.actor.Inbox;
/**
 * Created by adkhare on 4/3/2017.
 */
public class WorkflowTaskDispatcher
{
    /**
     *
     */
    public static final ActorSystem WorkflowActorSystem;
    public static final FiniteDuration DefaultActorTimeout;
    private static final Inbox inbox;
    private static final Logger _LOGGER;
    /*
         * Initialize the static members
         */
    static
    {
        DefaultActorTimeout = Duration.create(15, TimeUnit.SECONDS);
        _LOGGER = Logger.getLogger(WorkflowTaskDispatcher.class.getName());
        WorkflowActorSystem = ActorSystem.create("WorkflowActors");
        inbox = Inbox.create(WorkflowTaskDispatcher.WorkflowActorSystem);
    }

    public static WorkflowStartResponse dispenseShippingLabel(WorkflowStartRequest req)
    {
        _LOGGER.entering(WorkflowTaskDispatcher.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        WorkflowStartResponse result = (WorkflowStartResponse) getResponseFromWorkflowSupervisor(req);
        _LOGGER.exiting(WorkflowTaskDispatcher.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        return result;
    }

    private static Object getResponseFromWorkflowSupervisor(Object req)
    {
        Object result = null;
        try
        {
            inbox.send(WorkflowSupervisor.WorkflowActor, req);
            result = inbox.receive(WorkflowTaskDispatcher.DefaultActorTimeout);
        }
        catch (Exception ex)
        {
            _LOGGER.log(Level.WARNING, "Got a timeout waiting for reply from an actor");
        }
        _LOGGER.exiting(WorkflowTaskDispatcher.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        return result;
    }
}
