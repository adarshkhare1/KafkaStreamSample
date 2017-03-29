package org.adarshkhare.KafkaWorkflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkflowSupervisor
{
    private static final int DEFAULT_POLLER_COUNT = 3;
    private final String id ;
    private final List<Worker> pollers;

    private WorkflowSupervisor(String supervisorId, int numPoller)
    {
        if(numPoller < 1 || numPoller > 10)
        {
            numPoller = DEFAULT_POLLER_COUNT;
            Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.WARNING,
                    "numPoller="+numPoller+" are out of range, setting default to "+DEFAULT_POLLER_COUNT);
        }
        this.id = supervisorId;
        this.pollers = new ArrayList<>(numPoller);
        for (int i = 0; i < numPoller; i++)
        {
            Worker poller = null;
            try {
                poller = new Worker(this.id+"-"+i);
                this.pollers.add(poller);
            }
            catch (IOException e) {
                Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.SEVERE, "Failed to intialize poller");
                Logger.getLogger(WorkflowSupervisor.class.getName()).log(Level.SEVERE, e.toString());
            }
            this.pollers.add(poller);
        }
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
}
