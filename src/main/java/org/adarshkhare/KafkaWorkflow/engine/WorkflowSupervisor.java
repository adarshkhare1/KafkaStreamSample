package org.adarshkhare.KafkaWorkflow.engine;

import akka.actor.*;
import org.adarshkhare.KafkaWorkflow.task.Worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkflowSupervisor extends UntypedActor
{
    public static final ActorRef WorkflowActor;

    private static final int DEFAULT_POLLER_COUNT = 3;
    private final Logger _LOGGER;
    private final Inbox inbox;
    private final String id ;
    private final List<Worker> pollers;

    /*
	 * Initialize the static members
	 */
    static
    {
        ActorSystem wfActorSystem = WorkflowTaskDispatcher.WorkflowActorSystem;
        WorkflowActor = wfActorSystem.actorOf(Props.create(WorkflowSupervisor.class), "WorkflowSupervisor");
    }


    private WorkflowSupervisor(String supervisorId, int numPoller)
    {
        this._LOGGER = Logger.getLogger(WorkflowSupervisor.class.getName());
        this.inbox = Inbox.create(WorkflowTaskDispatcher.WorkflowActorSystem);

        if(numPoller < 1 || numPoller > 10)
        {
            numPoller = DEFAULT_POLLER_COUNT;
            _LOGGER.log(Level.WARNING,
                    "numPoller="+numPoller+" are out of range, setting default to "+DEFAULT_POLLER_COUNT);
        }
        this.id = supervisorId;
        this.pollers = new ArrayList<>(numPoller);
        this.InitializePollers(numPoller);
    }



    public static WorkflowSupervisor CreateSupervisor(String supervisorId)
    {
        return WorkflowSupervisor.CreateSupervisor(supervisorId, DEFAULT_POLLER_COUNT);
    }

    public static WorkflowSupervisor CreateSupervisor(String supervisorId, int numMessagePoller)
    {
        Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.INFO, "Initializing Workflow supervisor");
        Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.INFO, "supervisorId="+supervisorId);
        Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.INFO, "numMessagePoller="+numMessagePoller);
        WorkflowSupervisor supervisor = new WorkflowSupervisor(supervisorId, numMessagePoller);
        return supervisor;
    }

    @Override
    public void onReceive(Object message) throws Throwable
    {
        _LOGGER.entering(WorkflowSupervisor.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());

        inbox.send(WorkflowSupervisor.WorkflowActor, message);
        try
        {
            Object result = inbox.receive(WorkflowTaskDispatcher.DefaultActorTimeout);
            getSender().tell(result, getSelf());
        }
        catch (Exception ex)
        {
            unhandled(message);
            _LOGGER.log(Level.WARNING, "Got a timeout waiting for reply from an actor:"+ex.getMessage());
        }
        _LOGGER.exiting(WorkflowSupervisor.class.getName(), Thread.currentThread().getStackTrace()[0].getMethodName());
    }


    public void Start()
    {
        ExecutorService service = Executors.newFixedThreadPool(this.pollers.size());
        for (Worker poller:this.pollers)
        {
            if(poller != null)
            {
                poller.Subscribe();

                service.submit(poller);
            }
        }
    }

    public void Shutdown()
    {
        for (Worker poller:this.pollers)
        {
            if(poller != null)
            {
                poller.shutdown();
            }
        }
    }

    private void InitializePollers(int numPoller)
    {
        for (int i = 0; i < numPoller; i++)
        {
            Worker poller = null;
            try {
                poller = new Worker(this.id+"-"+i);
                this.pollers.add(poller);
            }
            catch (IOException e) {
                _LOGGER.log(Level.SEVERE, "Failed to intialize poller");
                _LOGGER.log(Level.SEVERE, e.toString());
            }
            this.pollers.add(poller);
        }
    }

}
