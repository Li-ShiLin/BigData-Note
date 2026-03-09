package com.action.flinkcdc;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 对应 sakila.actor 表，用于 CDC 后写入 Redis 的实体。
 * Debezium 输出为 snake_case，需用 @SerializedName 与 Gson 映射。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Actor {

    @SerializedName("actor_id")
    private Integer actorId;

    @SerializedName("first_name")
    private String firstName;

    @SerializedName("last_name")
    private String lastName;

    @SerializedName("last_update")
    private String lastUpdate;
}
