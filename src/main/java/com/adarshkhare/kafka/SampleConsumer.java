/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.kafka;
 
import com.google.common.io.Resources;
import kafka.consumer.ConsumerIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SampleConsumer implements Runnable{
    private KafkaConsumer<String,String> myConsumer;
 
    public SampleConsumer()
    {
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            this.myConsumer = new KafkaConsumer<String, String>(properties);
        }
        catch (Exception ex)
        {
            Logger.getLogger(SampleConsumer.class.getName()).log(Level.SEVERE, "Consumer Failed", ex);
        }
    }

    public void Subscribe()
    {
        this.myConsumer.subscribe(Arrays.asList("testing"));
        Logger.getLogger(SampleConsumer.class.getName()).log(Level.INFO, "Subscribed");
    }

    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                ConsumerRecords<String, String> records = this.myConsumer.poll(10000);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.out.println("Received Data: " + data);
                }
            }
        }
        catch (WakeupException e)
        {
            // ignore for shutdown
        }
        finally
        {
                this.myConsumer.close();
        }
    }

    public void shutdown() {
        this.myConsumer.wakeup();
    }
}
