package org.adarshkhare.KafkaWorkflow.engine;

import akka.actor.*;
import com.google.common.collect.Range;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.*;

public class WorkflowSupervisor
{
    private static final Logger _LOGGER;
    private static final int DEFAULT_POLLER_COUNT = 3;

    public final ActorRef ActivityRouter;
    private final Inbox inbox;
    private final String id ;
    private final List<MessagePoller> messagePollers;

    /*
	 * Initialize the static members
	 */
    static
    {
        _LOGGER = Logger.getLogger(WorkflowSupervisor.class.getName());
    }

    private WorkflowSupervisor(String supervisorId, int requestedNumPollers) throws IOException
    {
        ActorSystem routingSystem = WorkflowMessageRouter.MessageRoutingSystem;

        this.id = checkNotNull(supervisorId);
        this.messagePollers = this.InitializePollers(this.getNumPollers(requestedNumPollers));
        this.ActivityRouter = routingSystem.actorOf(Props.create(WorkflowSupervisor.class), "WorkflowSupervisor");
        this.inbox = Inbox.create(routingSystem);
    }

    public static WorkflowSupervisor CreateSupervisor(String supervisorId, int numMessagePoller) throws IOException
    {
        _LOGGER.log(Level.INFO, "Initializing Workflow supervisor");
        _LOGGER.log(Level.INFO, "supervisorId="+supervisorId);
        _LOGGER.log(Level.INFO, "numMessagePoller="+numMessagePoller);
        WorkflowSupervisor supervisor = new WorkflowSupervisor(supervisorId, numMessagePoller);
        return supervisor;
    }

    public static WorkflowSupervisor CreateSupervisor(String supervisorId) throws IOException
    {
        return WorkflowSupervisor.CreateSupervisor(supervisorId, DEFAULT_POLLER_COUNT);
    }

    public void Start()
    {
        ExecutorService service = Executors.newFixedThreadPool(this.messagePollers.size());
        for (MessagePoller poller:this.messagePollers)
        {
            if(poller != null)
            {
                poller.Subscribe();

                service.submit(poller);
            }
        }
    }

    public void SendMesage(ActivityRequest req)
    {
        _LOGGER.entering(WorkflowSupervisor.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
        Object result = null;
        try
        {
            inbox.send(this.ActivityRouter, req);
            result = inbox.receive(WorkflowMessageRouter.DefaultActorTimeout);
        }
        catch (Exception ex)
        {
            _LOGGER.log(Level.WARNING, "Got a timeout waiting for reply from an actor");
        }
        _LOGGER.exiting(WorkflowSupervisor.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
    }

    public void Shutdown()
    {
        for (MessagePoller poller:this.messagePollers)
        {
            if(poller != null)
            {
                poller.shutdown();
            }
        }
    }

    private int getNumPollers(int numPoller)
    {
        Range<Integer> pollerCountRange = Range.closed(1, 10);
        if(!pollerCountRange.contains(numPoller))
        {
            _LOGGER.log(Level.WARNING,
                    "numPoller="+numPoller+" are out of range, setting default to "+DEFAULT_POLLER_COUNT);
            return DEFAULT_POLLER_COUNT;
        }
        else
        {
            return numPoller;
        }
    }

    private List<MessagePoller> InitializePollers(int numPoller) throws IOException {
        List<MessagePoller> pollersList =  new ArrayList<>(numPoller);
        for (int i = 0; i < numPoller; i++)
        {
            MessagePoller poller = null;
            try
            {
                poller = new MessagePoller(this.id+"-"+i, this);
                pollersList.add(poller);
            }
            catch (IOException e)
            {
                _LOGGER.log(Level.SEVERE, "Failed to intialize poller");
                _LOGGER.log(Level.SEVERE, e.toString());
                throw e;
            }
        }
        return pollersList;
    }

}
