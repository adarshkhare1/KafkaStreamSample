/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class SampleProducer {
    private static final String SampleTopic  = "testing";
    private static final String BrokerAddress  = "10.1.1.12:9092";

    private KafkaProducer myProducer;

    public SampleProducer()
    {
        try
        {
            this.myProducer = new KafkaProducer<>(getProducerProperties());
        }
        catch (Exception ex)
        {
            Logger.getLogger(SampleProducer.class.getName()).log(Level.SEVERE, "Producer Failed", ex);
        }
    }




    /**
     * This simple producer will generate events such
     *                that each event is a random number.
     * @param nEvents number of events producer should generate.
     *
     */
    public void SendMessages(long nEvents)
    {
        Random rnd = new Random();
        try
        {
            for (long n = 0; n < nEvents; n++) {
                String messageValue = Long.toString(rnd.nextLong());
                this.SendMessage(messageValue);
                Logger.getLogger(SampleProducer.class.getName()).log(Level.INFO, "Message Sent");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(SampleProducer.class.getName()).log(Level.SEVERE, "Send Failed", ex);
        }
    }

    /**
     * Close the producer.
     */
    public void Close()
    {
        Producer p = myProducer;
        if(p != null)
        {
            p.close();
        }
    }

    private Properties getProducerProperties() {
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", BrokerAddress);

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }



    private void SendMessage(String messageValue) {
        ProducerRecord message = new ProducerRecord<>(SampleTopic, messageValue, messageValue);
        this.myProducer.send(message);
        this.myProducer.flush();
    }
}
