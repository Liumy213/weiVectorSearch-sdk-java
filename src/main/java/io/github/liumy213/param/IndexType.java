/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.liumy213.param;

import lombok.Getter;

public enum IndexType {
    INVALID,
    //Only supported for float vectors
    FLAT(1),
//    IVF_FLAT(2),
//    IVF_PQ(3),
//    HNSW(4),
    DISKANN(2),
    ;

    @Getter
    private final String name;

    @Getter
    private final int code;

    IndexType(){
        this.name = this.name();
        this.code = this.ordinal();
    }

    IndexType(int code){
        this.name = this.name();
        this.code = code;
    }

    IndexType(String name, int code){
        this.name = name;
        this.code = code;
    }
}
