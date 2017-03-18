/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.kafka;
import com.google.common.io.Resources;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class SampleProducer {
    private static final String SampleTopic  = "testing";

    private KafkaProducer myProducer;

    public SampleProducer()
    {
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            this.myProducer = new KafkaProducer<>(properties);
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
                ProducerRecord messageRecord
                        = new ProducerRecord<String, String>(SampleProducer.SampleTopic,
                        Long.toString(n), Long.toString(rnd.nextLong()));
                Future sendWait = this.myProducer.send(messageRecord);
                this.myProducer.flush();
                sendWait.wait(10000);
                if (sendWait.isDone())
                {
                    Logger.getLogger(SampleProducer.class.getName()).log(Level.INFO, "Message Sent Successful");

                }
                else
                {
                    Logger.getLogger(SampleProducer.class.getName()).log(Level.INFO, "Message Sent Fail");
                }
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

}
