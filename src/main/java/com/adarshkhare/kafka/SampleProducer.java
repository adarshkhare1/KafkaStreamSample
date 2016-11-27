/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.adarshkhare.kafka;

import java.util.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
 
public class SampleProducer {
    public static void TestProducer(String[] args) {
        long events = Long.parseLong(args[0]);
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "SampleProducer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        try (Producer<String, String> producer = new KafkaProducer<>(props))
        {
            for (long n = 0; n < events; n++) {
                ProducerRecord data;
                data = new ProducerRecord<>("my-topic", Long.toString(n), Long.toString(n));
                producer.send(data);
            }
        }
    }
}
