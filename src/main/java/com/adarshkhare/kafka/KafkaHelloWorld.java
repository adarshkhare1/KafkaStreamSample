package com.adarshkhare.kafka;

import org.apache.log4j.BasicConfigurator;
import sun.nio.ch.ThreadPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.System.exit;

/**
 * Created by adarshkhare on 3/11/17.
 */
public class KafkaHelloWorld {
    public static void main(String[] args) throws Exception
    {
        BasicConfigurator.configure();
        InitializeLogLevels();
        try
        {
            SampleProducer producer = new SampleProducer();
            producer.SendMessages(10);
            producer.Close();
        }
        catch (Exception ex)
        {
            Logger.getLogger(SampleProducer.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Now read all messages from consumer
        SampleConsumer consumer = new SampleConsumer();
        consumer.Subscribe();
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(consumer);
        Thread.sleep(10000);
        consumer.shutdown();
        exit(0);

    }

    private static void InitializeLogLevels()
    {
        //Override the logging levels
        Logger.getLogger("org").setLevel(Level.WARNING);
        Logger.getLogger("apache").setLevel(Level.WARNING);
        Logger.getLogger("akka").setLevel(Level.WARNING);
        Logger.getLogger("kafka").setLevel(Level.WARNING);
        Logger.getLogger("clients").setLevel(Level.WARNING);
        Logger.getLogger("producer").setLevel(Level.WARNING);
    }
}
