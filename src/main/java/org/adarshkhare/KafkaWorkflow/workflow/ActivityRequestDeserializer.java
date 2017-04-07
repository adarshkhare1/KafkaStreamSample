package org.adarshkhare.KafkaWorkflow.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;


public class ActivityRequestDeserializer implements Deserializer<ActivityRequest> {

    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    @Override
    public ActivityRequest deserialize(String s, byte[] value)
    {
        if (value == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        ActivityRequest activity = null;
        try {
            activity = mapper.readValue(value, ActivityRequest.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return activity;
    }

    @Override
    public void close()
    {

    }
}
