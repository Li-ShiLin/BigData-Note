package com.action.flinkcdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.Objects;

@Slf4j
public class CacheSink extends RichSinkFunction<String> {

    private static final String REDIS_ADDRESS = "redis://192.168.56.12:6379";

    /**
     * Debezium 操作类型：r=快照读, c=插入, u=更新, d=删除
     */
    private static final String OP_READ = "r";
    private static final String OP_CREATE = "c";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";

    /**
     * Debezium 中 op 节点路径，用于解析操作类型
     */
    private static final String PATH_OP = "op";
    /**
     * Debezium 中 after 节点路径，用于解析整行实体（set 缓存）
     */
    private static final String PATH_AFTER = "after";
    /**
     * Debezium 中 before 节点路径，用于解析整行实体（delete 缓存）
     */
    private static final String PATH_BEFORE = "before";

    private RedissonClient redissonClient;

    public CacheSink() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Config config = new Config();
            config.setCodec(StringCodec.INSTANCE);
            config.useSingleServer().setAddress(REDIS_ADDRESS).setConnectionPoolSize(10).setConnectionMinimumIdleSize(2);
            redissonClient = Redisson.create(config);
            log.info("Redis 客户端初始化成功，地址：{}", REDIS_ADDRESS);
        } catch (Exception e) {
            log.error("Redis 客户端初始化失败", e);
            throw new RuntimeException("Redis 连接失败", e);
        }
    }

    @Override
    public void invoke(String json, Context context) throws Exception {
        super.invoke(json, context);
        log.info("接收到 CDC 事件：{}", json);

        try {
            String op = JsonUtils.transferToEntity(json, PATH_OP, String.class);
            if (Objects.isNull(op)) {
                log.info("CDC 事件解析 op 为空，JSON：{}", json);
                return;
            }
            log.info("解析到操作类型：op={}", op);

            Actor actor;
            switch (op) {
                case OP_READ:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_READ);
                    break;
                case OP_CREATE:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_CREATE);
                    break;
                case OP_UPDATE:
                    actor = JsonUtils.transferToEntity(json, PATH_AFTER, Actor.class);
                    setCache(actor, OP_UPDATE);
                    break;
                case OP_DELETE:
                    actor = JsonUtils.transferToEntity(json, PATH_BEFORE, Actor.class);
                    deleteCache(actor, OP_DELETE);
                    break;
                default:
                    log.error("忽略未知操作类型：op={}, JSON={}", op, json);
            }
        } catch (Exception e) {
            log.error("处理 CDC 事件失败，JSON：{}", json, e);
        }
    }

    private void setCache(Actor actor, String op) {
        if (Objects.isNull(actor) || Objects.isNull(actor.getActorId())) {
            return;
        }
        try {
            String key = "actor:" + actor.getActorId();
            RBucket<String> bucket = redissonClient.getBucket(key);
            bucket.set(JsonUtils.toJson(actor));
            log.info("缓存写入成功：op={}, key={}, actor={}", op, key, actor);
        } catch (Exception e) {
            log.error("缓存写入失败：op={}, actorId={}", op, actor.getActorId(), e);
        }
    }

    private boolean deleteCache(Actor actor, String op) {
        if (Objects.isNull(actor) || Objects.isNull(actor.getActorId())) {
            return false;
        }
        try {
            String key = "actor:" + actor.getActorId();
            RBucket<String> bucket = redissonClient.getBucket(key);
            boolean deleted = bucket.delete();
            log.info("缓存删除结果：op={}, key={}, deleted={}", op, key, deleted);
            return deleted;
        } catch (Exception e) {
            log.error("缓存删除失败：op={}, actorId={}", op, actor.getActorId(), e);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (Objects.nonNull(redissonClient)) {
            try {
                redissonClient.shutdown();
                log.info("Redis 客户端已关闭");
            } catch (Exception e) {
                log.error("Redis 客户端关闭失败", e);
            }
        }
    }
}
