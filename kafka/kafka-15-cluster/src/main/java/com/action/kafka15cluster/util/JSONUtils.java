package com.action.kafka15cluster.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONUtils {

    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();

    public static String toJSON(Object object) {
        try {
            return OBJECTMAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T toBean(String json, Class<T> clazz) {
        try {
            return OBJECTMAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}


