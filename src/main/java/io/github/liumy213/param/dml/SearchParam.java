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

import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.ParamUtils;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>search</code> interface.
 */
@Getter
public class SearchParam {
    private final String collectionName;
    private final String vectorFieldName;
    private final String textFieldName;
    private final int topK;
    private final String expr;
    private final List<String> outFields;
    private final List<?> searchData;
    private final Long NQ;
    private final String params;

    private SearchParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.vectorFieldName = builder.vectorFieldName;
        this.textFieldName = builder.textFieldName;
        this.topK = builder.topK;
        this.expr = builder.expr;
        this.outFields = builder.outFields;
        this.searchData = builder.searchData;
        this.NQ = builder.NQ;
        this.params = builder.params;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link SearchParam} class.
     */
    public static class Builder {
        private String collectionName;
        private String vectorFieldName;
        private String textFieldName;
        private Integer topK;
        private String expr = "";
        private final List<String> outFields = new ArrayList<>();
        private List<?> searchData;
        private Long NQ;
        private String params = "{}";

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
         * @param searchData list of target vectors or texts:
         *                if type is List, searchData is List of List Float;
         *                if type is String, vectors is List of String;
         * @return <code>Builder</code>
         */
        public Builder withSearchData(@NonNull List<?> searchData) {
            this.searchData = searchData;
            this.NQ = (long) searchData.size();
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

            if (vectorFieldName != null && !StringUtils.isBlank(vectorFieldName)) {
                if (searchData == null || searchData.isEmpty()) {
                    throw new ParamException("Target vectors can not be empty");
                }

                if (searchData.get(0) instanceof List) {
                    // float vectors
                    List<?> first = (List<?>) searchData.get(0);
                    if (!(first.get(0) instanceof Float)) {
                        throw new ParamException("Float vector field's value must be Lst<Float>");
                    }

                    int dim = first.size();
                    for (int i = 1; i < searchData.size(); ++i) {
                        List<?> temp = (List<?>) searchData.get(i);
                        if (dim != temp.size()) {
                            throw new ParamException("Target vector dimension must be equal");
                        }
                    }
                } else {
                    throw new ParamException("Target vector type must be List<Float>");
                }
            }

            if (textFieldName != null && !StringUtils.isBlank(textFieldName)) {
                if (searchData == null || searchData.isEmpty()) {
                    throw new ParamException("Target texts can not be empty");
                }

                if (!(searchData.get(0) instanceof String)) {
                    throw new ParamException("Target search data type must be List<String>");
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
        return "SearchParam{" +
                "collectionName='" + collectionName + '\'' +
                ", target vectors count=" + searchData.size() +
                ", vectorFieldName='" + vectorFieldName + '\'' +
                ", topK=" + topK +
                ", nq=" + NQ +
                ", expr='" + expr + '\'' +
                ", params='" + params + '\'' +
                '}';
    }
}
