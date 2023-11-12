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

package io.github.liumy213.param.index;

import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.Constant;
import io.github.liumy213.param.IndexType;
import io.github.liumy213.param.MetricType;
import io.github.liumy213.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Parameters for <code>createIndex</code> interface.
 */
@Getter
@ToString
public class CreateIndexParam {
    private final String collectionName;
    private final String fieldName;
    private final String indexName;
    private final IndexType indexType; // for easily get to check with field type
    private final Map<String, String> extraParam = new HashMap<>();

    private CreateIndexParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.fieldName = builder.fieldName;
        this.indexName = builder.indexName;
        this.indexType = builder.indexType;
        if (builder.indexType != IndexType.INVALID) {
            this.extraParam.put(Constant.INDEX_TYPE, builder.indexType.getName());
        }
        if (builder.metricType != MetricType.INVALID) {
            this.extraParam.put(Constant.METRIC_TYPE, builder.metricType.name());
        }
        if (builder.extraParam != null) {
            this.extraParam.put(Constant.PARAMS, builder.extraParam);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateIndexParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String fieldName;
        private IndexType indexType = IndexType.INVALID;
        private String indexName = Constant.DEFAULT_INDEX_NAME;
        private MetricType metricType = MetricType.INVALID;
        private String extraParam;

        private Builder() {
        }

        /**
         * Set the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets the target field name. Field name cannot be empty or null.
         *
         * @param fieldName field name
         * @return <code>Builder</code>
         */
        public Builder withFieldName(@NonNull String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * Sets the index type.
         *
         * @param indexType index type
         * @return <code>Builder</code>
         */
        public Builder withIndexType(@NonNull IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        /**
         * The name of index which will be created. Then you can use the index name to check the state of index.
         * If no index name is specified, the default index name("_default_idx") is used.
         *
         * @param indexName index name
         * @return <code>Builder</code>
         */
        public Builder withIndexName(@NonNull String indexName) {
            this.indexName = indexName;
            return this;
        }

        /**
         * Sets the metric type.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets the specific index parameters according to index type.
         *
         * For example: IVF index, the extra parameters can be "{\"nlist\":1024}".
         *
         * @param extraParam extra parameters in .json format
         * @return <code>Builder</code>
         */
        public Builder withExtraParam(@NonNull String extraParam) {
            this.extraParam = extraParam;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateIndexParam} instance.
         *
         * @return {@link CreateIndexParam}
         */
        public CreateIndexParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            ParamUtils.CheckNullEmptyString(fieldName, "Field name");

            if (indexName == null || StringUtils.isBlank(indexName)) {
                indexName = Constant.DEFAULT_INDEX_NAME;
            }

            if (indexType == IndexType.INVALID) {
                throw new ParamException("Index type is required");
            }

            if (ParamUtils.IsVectorIndex(indexType)) {
                if (metricType == MetricType.INVALID) {
                    throw new ParamException("Metric type is required");
                }
            }

            return new CreateIndexParam(this);
        }
    }
}
