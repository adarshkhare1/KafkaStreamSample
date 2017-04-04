package org.adarshkhare.KafkaWorkFlowTest;

import org.adarshkhare.KafkaWorkflow.WorkflowTaskFeeder;
import org.adarshkhare.KafkaWorkflow.engine.WorkflowSupervisor;

import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.System.exit;

/**
 * Created by adarshkhare on 3/11/17.
 */
public class KafkaHelloWorld
{
    public static void main(String[] args) throws Exception
    {
        //BasicConfigurator.configure();
        //Now launch supervisor to process messages
        WorkflowSupervisor supervisor = WorkflowSupervisor.CreateSupervisor("TestSupervisor", 3);
        supervisor.Start();
        KafkaHelloWorld.FeedMessagesToWorkflow();
        Thread.sleep(1000);
        supervisor.Shutdown();
        exit(0);
    }

    private static void FeedMessagesToWorkflow() {
        try
        {
            WorkflowTaskFeeder taskFeeder = new WorkflowTaskFeeder();
            taskFeeder.SendMessages(5);
            taskFeeder.Close();
        }
        catch (Exception ex)
        {
            Logger.getLogger(WorkflowTaskFeeder.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
