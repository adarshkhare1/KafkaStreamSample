package org.adarshkhare.KafkaWorkflow;

import com.google.common.io.Resources;
import org.adarshkhare.KafkaWorkflow.workflow.ActivityRequest;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkflowTaskFeeder {
    public static final String SampleTopic  = "MyTestTopic-workflow";

    private final Logger _LOGGER;
    private final KafkaProducer myProducer;

    public static final String USER_SCHEMA = "";
    //private static final SpecificDatumWriter<Event> avroEventWriter = new SpecificDatumWriter<Event>(Event.SCHEMA$);
    public WorkflowTaskFeeder() throws IOException
    {
        _LOGGER =  LogManager.getLogger(WorkflowTaskFeeder.class.getName());
        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            this.myProducer = new KafkaProducer<>(properties);
        }
        catch (IOException ex)
        {
            _LOGGER.fatal("Failed to find config for taskfeeder.", ex);
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
            for (long n = 0; n < nEvents; n++)
            {
                ActivityRequest activity = new ActivityRequest(Long.toString(n));
                activity.setPayload(Long.toString(rnd.nextInt()));
                ProducerRecord messageRecord
                        = new ProducerRecord<>(WorkflowTaskFeeder.SampleTopic,
                        Long.toString(n), activity);
                Future sendWait = this.myProducer.send(messageRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e)
                    {
                        if (e != null) {
                            _LOGGER.fatal("Message Sent Fail : %s", e);
                        }
                        _LOGGER.info( "Message Sent Successful");
                    }
                });
                this.myProducer.flush();
            }
        }
        catch (Exception ex)
        {
            _LOGGER.fatal(ex);
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
