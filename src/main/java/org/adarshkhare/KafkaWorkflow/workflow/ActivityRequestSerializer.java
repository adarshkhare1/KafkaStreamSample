package org.adarshkhare.KafkaWorkflow.workflow;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class ActivityRequestSerializer implements Serializer<ActivityRequest>
{
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, ActivityRequest message)
    {
        if (message == null)
        {
            return null;
        }
        try
        {
            byte[] retVal = null;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                retVal = objectMapper.writeValueAsString(message).getBytes();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return retVal;
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }
    }

    @Override
    public void close()
    {

    }
}
