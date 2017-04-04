/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.adarshkhare.KafkaWorkflow.engine;
 
import com.google.common.io.Resources;
import org.adarshkhare.KafkaWorkflow.WorkflowTaskFeeder;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessagePoller implements Runnable{
    private final KafkaConsumer<String,ActivityRequest> myConsumer;
    private final String consumerId;
    private final WorkflowSupervisor parentSupervisor;

    //Keep track of number of messages received by this worker.
    private int numMessageReceived;
 
    public MessagePoller(String id, WorkflowSupervisor parent) throws IOException {
        this.consumerId = id;
        this.parentSupervisor = parent;
        try (InputStream props = Resources.getResource("consumer.properties").openStream())
        {
            Properties properties = new Properties();
            properties.load(props);
            properties.put("group.id", id);
            this.myConsumer = new KafkaConsumer<String, ActivityRequest>(properties);
        }
    }

    public void Subscribe()
    {
        this.myConsumer.subscribe(Arrays.asList(WorkflowTaskFeeder.SampleTopic));
        Logger.getLogger(MessagePoller.class.getName()).log(Level.INFO, this.consumerId+":Subscribed");
    }

    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                ConsumerRecords<String, ActivityRequest> records = this.myConsumer.poll(10000);
                for (ConsumerRecord<String, ActivityRequest> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    Logger.getLogger(MessagePoller.class.getName()).log(Level.INFO, this.consumerId+"-Received: " + data);
                    this.parentSupervisor.SendMesage(record.value());
                    this.numMessageReceived++;
                }
            }
        }
        catch (WakeupException e)
        {
            // ignore for shutdown
        }
        finally
        {
            Logger.getLogger(MessagePoller.class.getName()).log(Level.INFO,
                    this.consumerId+"- NumMessagesReceived = " + this.numMessageReceived);
                    this.myConsumer.close();
        }
    }

    public void shutdown() {

        this.myConsumer.wakeup();
    }
}
