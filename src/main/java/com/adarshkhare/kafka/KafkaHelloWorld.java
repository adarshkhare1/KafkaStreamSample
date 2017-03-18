package com.adarshkhare.kafka;

import org.apache.log4j.BasicConfigurator;

import java.util.logging.Level;
import java.util.logging.Logger;

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
            producer.SendMessages(1);
            producer.Close();
        }
        catch (Exception ex)
        {
            Logger.getLogger(SampleProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
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
