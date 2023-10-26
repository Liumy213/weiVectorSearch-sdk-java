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

package io.github.liumy213.param.dml;

import com.baidu.cloud.thirdparty.google.common.collect.Lists;
import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.Constant;
import io.github.liumy213.param.MetricType;
import io.github.liumy213.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
@Getter
public class SearchParam {
    private final String collectionName;
    private final List<String> partitionNames;
    private final String metricType;
    private final String vectorFieldName;
    private final String textFieldName;
    private final int topK;
    private final String expr;
    private final List<String> outFields;
    private final List<?> vectors;
    private final List<String> texts;
    private final Long NQ;
    private final int roundDecimal;
    private final String params;
    private final long travelTimestamp;
    private final long guaranteeTimestamp;
    private final Long gracefulTime;
    private final boolean ignoreGrowing;

    private SearchParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionNames = builder.partitionNames;
        this.metricType = builder.metricType.name();
        this.vectorFieldName = builder.vectorFieldName;
        this.textFieldName = builder.textFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.outFields = builder.outFields;
        this.vectors = builder.vectors;
        this.texts = builder.texts;
        this.NQ = builder.NQ;
        this.roundDecimal = builder.roundDecimal;
        this.params = builder.params;
        this.travelTimestamp = builder.travelTimestamp;
        this.guaranteeTimestamp = builder.guaranteeTimestamp;
        this.gracefulTime = builder.gracefulTime;
        this.ignoreGrowing = builder.ignoreGrowing;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link SearchParam} class.
     */
    public static class Builder {
        private String collectionName;
        private final List<String> partitionNames = Lists.newArrayList();
        private MetricType metricType = MetricType.L2;
        private String vectorFieldName;
        private String textFieldName;
        private Integer topK;
        private String expr = "";
        private final List<String> outFields = Lists.newArrayList();
        private List<?> vectors;
        private List<String> texts;
        private Long NQ;
        private Integer roundDecimal = -1;
        private String params = "{}";
        private Long travelTimestamp = 0L;
        private Long guaranteeTimestamp = Constant.GUARANTEE_EVENTUALLY_TS;
        private Long gracefulTime = 5000L;
        private Boolean ignoreGrowing = Boolean.FALSE;

       Builder() {
        }

        /**
         * Sets the collection name. Collection name cannot be empty or null.
         *
         * @param collectionName collection name
         * @return <code>Builder</code>
         */
        public Builder withCollectionName(@NonNull String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        /**
         * Sets partition names list to specify search scope (Optional).
         *
         * @param partitionNames partition names list
         * @return <code>Builder</code>
         */
        public Builder withPartitionNames(@NonNull List<String> partitionNames) {
            partitionNames.forEach(this::addPartitionName);
            return this;
        }

        /**
         * Adds a partition to specify search scope (Optional).
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder addPartitionName(@NonNull String partitionName) {
            if (!this.partitionNames.contains(partitionName)) {
                this.partitionNames.add(partitionName);
            }
            return this;
        }

        /**
         * Sets metric type of ANN searching.
         *
         * @param metricType metric type
         * @return <code>Builder</code>
         */
        public Builder withMetricType(@NonNull MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        /**
         * Sets target vector field by name. Field name cannot be empty or null.
         *
         * @param vectorFieldName vector field name
         * @return <code>Builder</code>
         */
        public Builder withVectorFieldName(@NonNull String vectorFieldName) {
            this.vectorFieldName = vectorFieldName;
            return this;
        }

        /**
         * Sets target text field by name. Field name cannot be empty or null.
         * @param textFieldName text field name
         * @return <code>Builder</code>
         */
        public Builder withTextFieldName(@NonNull String textFieldName) {
            this.textFieldName = textFieldName;
            return this;
        }

        /**
         * Sets topK value of ANN search.
         *
         * @param topK topK value
         * @return <code>Builder</code>
         */
        public Builder withTopK(@NonNull Integer topK) {
            this.topK = topK;
            return this;
        }

        /**
         * Sets expression to filter out entities before searching (Optional).
         *
         * @param expr filtering expression
         * @return <code>Builder</code>
         */
        public Builder withExpr(@NonNull String expr) {
            this.expr = expr;
            return this;
        }

        /**
         * Specifies output fields (Optional).
         *
         * @param outFields output fields
         * @return <code>Builder</code>
         */
        public Builder withOutFields(@NonNull List<String> outFields) {
            outFields.forEach(this::addOutField);
            return this;
        }

        /**
         * Specifies an output field (Optional).
         *
         * @param fieldName filed name
         * @return <code>Builder</code>
         */
        public Builder addOutField(@NonNull String fieldName) {
            if (!this.outFields.contains(fieldName)) {
                this.outFields.add(fieldName);
            }
            return this;
        }

        /**
         * Sets the target vectors.
         *
         * @param vectors list of target vectors:
         *                if vector type is FloatVector, vectors is List of List Float;
         *                if vector type is BinaryVector, vectors is List of ByteBuffer;
         * @return <code>Builder</code>
         */
        public Builder withVectors(@NonNull List<?> vectors) {
            this.vectors = vectors;
            this.NQ = (long) vectors.size();
            return this;
        }

        /**
         * Sets the target texts.
         *
         * @param texts list of target texts:
         *                if text type is String, texts is List of String;
         * @return <code>Builder</code>
         */
        public Builder withTexts(@NonNull List<String> texts) {
            this.texts = texts;
            this.NQ = (long) texts.size();
            return this;
        }

        /**
         * Specifies the decimal place of the returned results.
         *
         * @param decimal how many digits after the decimal point
         * @return <code>Builder</code>
         */
        public Builder withRoundDecimal(@NonNull Integer decimal) {
            this.roundDecimal = decimal;
            return this;
        }

        /**
         * Sets the search parameters specific to the index type.
         *
         * For example: IVF index, the search parameters can be "{\"nprobe\":10}"
         *
         * @param params extra parameters in json format
         * @return <code>Builder</code>
         */
        public Builder withParams(@NonNull String params) {
            this.params = params;
            return this;
        }

        /**
         * Ignore the growing segments to get best search performance. Default is False.
         * For the user case that don't require data visibility.
         *
         * @param ignoreGrowing <code>Boolean.TRUE</code> ignore, Boolean.FALSE is not
         * @return <code>Builder</code>
         */
        public Builder withIgnoreGrowing(@NonNull Boolean ignoreGrowing) {
            this.ignoreGrowing = ignoreGrowing;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link SearchParam} instance.
         *
         * @return {@link SearchParam}
         */
        public SearchParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");
            if ((vectorFieldName == null || StringUtils.isBlank(vectorFieldName))
                    && (textFieldName == null || StringUtils.isBlank(textFieldName))) {
                throw new ParamException("Target field name cannot be null or empty, vectorField or textField chose one");
            } else if (vectorFieldName != null && textFieldName != null) {
                throw new ParamException("The target field name cannot be searched at the same time, vectorField or textField chose one");
            }

            if (topK <= 0) {
                throw new ParamException("TopK value is illegal");
            }

            if (travelTimestamp < 0) {
                throw new ParamException("The travel timestamp must be greater than 0");
            }

            if (guaranteeTimestamp < 0) {
                throw new ParamException("The guarantee timestamp must be greater than 0");
            }

            if (metricType == MetricType.INVALID) {
                throw new ParamException("Metric type is invalid");
            }

            if (vectorFieldName != null && !StringUtils.isBlank(vectorFieldName)) {
                if (vectors == null || vectors.isEmpty()) {
                    throw new ParamException("Target vectors can not be empty");
                }

                if (vectors.get(0) instanceof List) {
                    // float vectors
                    List<?> first = (List<?>) vectors.get(0);
                    if (!(first.get(0) instanceof Float)) {
                        throw new ParamException("Float vector field's value must be Lst<Float>");
                    }

                    int dim = first.size();
                    for (int i = 1; i < vectors.size(); ++i) {
                        List<?> temp = (List<?>) vectors.get(i);
                        if (dim != temp.size()) {
                            throw new ParamException("Target vector dimension must be equal");
                        }
                    }

                    // check metric type
                    if (!ParamUtils.IsFloatMetric(metricType)) {
                        throw new ParamException("Target vector is float but metric type is incorrect");
                    }
                } else if (vectors.get(0) instanceof ByteBuffer) {
                    // binary vectors
                    ByteBuffer first = (ByteBuffer) vectors.get(0);
                    int dim = first.position();
                    for (int i = 1; i < vectors.size(); ++i) {
                        ByteBuffer temp = (ByteBuffer) vectors.get(i);
                        if (dim != temp.position()) {
                            throw new ParamException("Target vector dimension must be equal");
                        }
                    }

                    // check metric type
                    if (!ParamUtils.IsBinaryMetric(metricType)) {
                        throw new ParamException("Target vector is binary but metric type is incorrect");
                    }
                } else {
                    throw new ParamException("Target vector type must be List<Float> or ByteBuffer");
                }
            }

            if (textFieldName != null && !StringUtils.isBlank(textFieldName)) {
                if (texts == null || texts.isEmpty()) {
                    throw new ParamException("Target texts can not be empty");
                }
            }

            return new SearchParam(this);
        }
    }

    /**
     * Constructs a <code>String</code> by {@link SearchParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        if (vectorFieldName != null && !StringUtils.isBlank(vectorFieldName)) {
            return "SearchParam{" +
                    "collectionName='" + collectionName + '\'' +
                    ", partitionNames='" + partitionNames.toString() + '\'' +
                    ", metricType=" + metricType +
                    ", target vectors count=" + vectors.size() +
                    ", vectorFieldName='" + vectorFieldName + '\'' +
                    ", topK=" + topK +
                    ", nq=" + NQ +
                    ", expr='" + expr + '\'' +
                    ", params='" + params + '\'' +
                    ", ignoreGrowing='" + ignoreGrowing + '\'' +
                    '}';
        } else {
            return "SearchParam{" +
                    "collectionName='" + collectionName + '\'' +
                    ", partitionNames='" + partitionNames.toString() + '\'' +
                    ", metricType=" + metricType +
                    ", target text count=" + texts.size() +
                    ", vectorFieldName='" + textFieldName + '\'' +
                    ", topK=" + topK +
                    ", nq=" + NQ +
                    ", expr='" + expr + '\'' +
                    ", params='" + params + '\'' +
                    ", ignoreGrowing='" + ignoreGrowing + '\'' +
                    '}';
        }
    }
}
