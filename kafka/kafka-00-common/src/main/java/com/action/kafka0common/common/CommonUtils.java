package com.action.kafka0common.common;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;

public class CommonUtils {

    public static <T> T convertString(String object, Class<T> c) {
        if (StringUtils.isEmpty(object)) {
            return null;
        }
        return JSONObject.parseObject(object, c);
    }


    public static <T> T convert(Object object, Class<T> c) {
        if (Objects.isNull(object)) {
            return null;
        }
        return JSONObject.parseObject(JSONObject.toJSONString(object), c);
    }

    public static <T> List<T> convertList(Object object, Class<T> c) {
        return JSON.parseArray(JSONObject.toJSONString(object), c);
    }


}
