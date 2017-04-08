package org.adarshkhare.KafkaWorkFlowTest;

import org.adarshkhare.KafkaWorkflow.WorkflowTaskFeeder;
import org.adarshkhare.KafkaWorkflow.engine.WorkflowSupervisor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.lang.System.exit;

/**
 * Created by adarshkhare on 3/11/17.
 */
public class KafkaHelloWorld
{
    public static void main(String[] args) throws Exception
    {
        int nMessages = 1;
        //Now launch supervisor to process messages
        WorkflowSupervisor supervisor = WorkflowSupervisor.CreateSupervisor("WorkflowSupervisor", 1);
        supervisor.Start();
        KafkaHelloWorld.FeedMessagesToWorkflow(nMessages);
        Thread.sleep(10000+nMessages*100);
        supervisor.Shutdown();
        exit(0);
    }

    private static void FeedMessagesToWorkflow(int nMessages) {
        try
        {
            WorkflowTaskFeeder taskFeeder = new WorkflowTaskFeeder();
            taskFeeder.SendMessages(nMessages);
            taskFeeder.Close();
        }
        catch (Exception ex)
        {
            LogManager.getLogger(KafkaHelloWorld.class.getName()).fatal(ex);
        }
    }
}
