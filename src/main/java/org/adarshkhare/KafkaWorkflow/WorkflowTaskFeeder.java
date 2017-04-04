package org.adarshkhare.KafkaWorkflow;

import com.google.common.io.Resources;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
 
public class WorkflowTaskFeeder {
    public static final String SampleTopic  = "MyTestTopic";

    private final Logger _LOGGER;
    private final KafkaProducer myProducer;

    public WorkflowTaskFeeder() throws IOException
    {
        _LOGGER =  Logger.getLogger(WorkflowTaskFeeder.class.getName());
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            this.myProducer = new KafkaProducer<>(properties);
        }
        catch (IOException ex)
        {
            _LOGGER.log(Level.SEVERE, "Failed to find config for taskfeeder.", ex);
            throw ex;
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
                        = new ProducerRecord<String, String>(WorkflowTaskFeeder.SampleTopic,
                        Long.toString(n), "Message:"+Long.toString(rnd.nextInt(1000000)));
                Future sendWait = this.myProducer.send(messageRecord);
                this.myProducer.flush();
                if (sendWait.isDone())
                {
                    _LOGGER.log(Level.INFO, "Message Sent Successful");

                }
                else
                {
                    _LOGGER.log(Level.INFO, "Message Sent Fail");
                }
            }
        }
        catch (Exception ex)
        {
            _LOGGER.log(Level.SEVERE, "Send Failed", ex.toString());
            throw ex;
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
