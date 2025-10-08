package com.action.kafka15cluster.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class User implements Serializable {

    private int id;

    private String phone;

    private Date birthDay;
}


