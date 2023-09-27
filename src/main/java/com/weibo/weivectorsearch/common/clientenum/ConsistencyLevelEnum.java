package com.weibo.weivectorsearch.common.clientenum;

import lombok.Getter;

public enum ConsistencyLevelEnum {

    STRONG("Strong", 0),
    // Session level is not allowed here because no ORM is implemented
//    SESSION("Session", 1),
    BOUNDED("Bounded", 2),
    EVENTUALLY("Eventually",3),
    ;

    @Getter
    private final String name;

    @Getter
    private final int code;

    ConsistencyLevelEnum(String name, int code){
        this.name = name;
        this.code = code;
    }

    private static final ConsistencyLevelEnum[] CONSISTENCY_LEVELS = new ConsistencyLevelEnum[values().length];

    public static ConsistencyLevelEnum getNameByCode(int code) {
        if (code >= 0 && code < CONSISTENCY_LEVELS.length) {
            return CONSISTENCY_LEVELS[code];
        }
        return null;
    }


}