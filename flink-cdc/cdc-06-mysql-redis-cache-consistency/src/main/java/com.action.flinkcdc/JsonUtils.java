package com.action.flinkcdc;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import ognl.Ognl;
import ognl.OgnlContext;

import java.util.Map;
import java.util.Objects;

@Slf4j
public class JsonUtils {

    private static final Gson GSON = new Gson();

    /**
     * 将指定JSON转为Map对象，Key固定为String，对应JSONkey
     * Value分情况：
     * 1. Value是字符串，自动转为字符串,例如:{"a","b"}
     * 2. Value是其他JSON对象，自动转为Map，例如：{"a":{"b":"2"}}}
     * 3. Value是数组，自动转为List<Map>，例如：{"a":[{"b":"2"},"c":"3"]}
     *
     * @param json 输入的JSON对象
     * @return 动态的Map集合
     */
    public static Map<String, Object> transferToMap(String json) {
        try {
            return GSON.fromJson(json, new TypeToken<Map<String, Object>>() {
            }.getType());
        } catch (Exception e) {
            log.error("JSON 转 Map 失败，json={}", json, e);
            return null;
        }
    }

    /**
     * 按OGNL路径解析JSON并转为指定类型
     *
     * @param json  原始的JSON数据
     * @param path  OGNL规则表达式
     * @param clazz Value对应的目标类
     * @return clazz对应数据
     */
    public static <T> T transferToEntity(String json, String path, Class<T> clazz) {
        try {
            Map<String, Object> root = transferToMap(json);
            OgnlContext ctx = new OgnlContext();
            ctx.setRoot(root);
            Object node = Ognl.getValue(path, ctx, ctx.getRoot());
            if (Objects.isNull(node)) {
                return null;
            }
            return GSON.fromJson(GSON.toJson(node), clazz);
        } catch (Exception e) {
            log.error("解析 JSON 失败：path={}, json={}", path, json, e);
            return null;
        }
    }

    /**
     * 将任意对象序列化为 JSON 字符串
     */
    public static String toJson(Object src) {
        return Objects.nonNull(src) ? GSON.toJson(src) : null;
    }
}
