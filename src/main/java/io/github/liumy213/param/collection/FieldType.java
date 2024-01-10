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

package io.github.liumy213.param.collection;

import io.github.liumy213.exception.ParamException;
import io.github.liumy213.param.Constant;
import io.github.liumy213.param.ParamUtils;
import io.github.liumy213.rpc.DataType;
import io.github.liumy213.rpc.ModelType;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Parameters for a collection field.
 */
@Getter
public class FieldType {
    private final String name;
    private final String description;
    private final DataType dataType;
    private final ModelType modelType;
    private final Map<String,String> typeParams;

    private FieldType(@NonNull Builder builder){
        this.name = builder.name;
        this.description = builder.description;
        this.dataType = builder.dataType;
        this.modelType = builder.modelType;
        this.typeParams = builder.typeParams;
    }

    public int getDimension() {
        if (typeParams.containsKey(Constant.VECTOR_DIM)) {
            return Integer.parseInt(typeParams.get(Constant.VECTOR_DIM));
        }

        return 0;
    }

    public int getMaxLength() {
        if (typeParams.containsKey(Constant.MAX_LENGTH)) {
            return Integer.parseInt(typeParams.get(Constant.MAX_LENGTH));
        }

        return 0;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link FieldType} class.
     */
    public static final class Builder {
        private String name;
        private String description = "";
        private DataType dataType;
        private ModelType modelType;
        private final Map<String,String> typeParams = new HashMap<>();

        private Builder() {
        }

        public Builder withName(@NonNull String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the field description. The description can be empty. The default is "".
         *
         * @param description description of the field
         * @return <code>Builder</code>
         */
        public Builder withDescription(@NonNull String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the data type for the field.
         *
         * @param dataType data type of the field
         * @return <code>Builder</code>
         */
        public Builder withDataType(@NonNull DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        /**
         * Sets the model type for text to vector.
         *
         * @param modelType model type
         * @return <code>Builder</code>
         */
        public Builder withModelType(ModelType modelType) {
            this.modelType = modelType;
            return this;
        }

        /**
         * Adds a parameter pair for the field.
         *
         * @param key parameter key
         * @param value parameter value
         * @return <code>Builder</code>
         */
        public Builder addTypeParam(@NonNull String key, @NonNull String value) {
            this.typeParams.put(key, value);
            return this;
        }

        /**
         * Sets more parameters for the field.
         *
         * @param typeParams parameters of the field
         * @return <code>Builder</code>
         */
        public Builder withTypeParams(@NonNull Map<String, String> typeParams) {
            typeParams.forEach(this.typeParams::put);
            return this;
        }

        /**
         * Sets the dimension of a vector field. Dimension value must be greater than zero.
         *
         * @param dimension dimension of the field
         * @return <code>Builder</code>
         */
        public Builder withDimension(@NonNull Integer dimension) {
            this.typeParams.put(Constant.VECTOR_DIM, dimension.toString());
            return this;
        }

        /**
         * Sets the max length of a varchar field. The value must be greater than zero.
         *
         * @param maxLength max length of a varchar field
         * @return <code>Builder</code>
         */
        public Builder withMaxLength(@NonNull Integer maxLength) {
            this.typeParams.put(Constant.MAX_LENGTH, maxLength.toString());
            return this;
        }

        /**
         * Sets whether normalization should be performed.
         * @param isBatchNormalize bool of normalization
         * @return <code>Builder</code>
         */
        public Builder withBatchNormalize(@NonNull boolean isBatchNormalize) {
            this.typeParams.put(Constant.BATCH_NORMALIZE, Boolean.toString(isBatchNormalize));
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link FieldType} instance.
         *
         * @return {@link FieldType}
         */
        public FieldType build() throws ParamException {
            ParamUtils.CheckNullEmptyString(name, "Field name");

            if (dataType == null || dataType == DataType.None) {
                throw new ParamException("Field data type is illegal");
            }

            if (dataType == DataType.String) {
                if (modelType == null) {
                    modelType = ModelType.SIMCSE;
                } else if (modelType == ModelType.UNRECOGNIZED) {
                    modelType = ModelType.SIMCSE;
                }
            } else {
                if (modelType != null) {
                    throw new ParamException("DataType is not string, modelType is not allowed to be set");
                }
                modelType = ModelType.NONE;
            }

            if (dataType == DataType.FloatVector) {
                if (!typeParams.containsKey(Constant.VECTOR_DIM)) {
                    throw new ParamException("Vector field dimension must be specified");
                }

                try {
                    int dim = Integer.parseInt(typeParams.get(Constant.VECTOR_DIM));
                    if (dim <= 0) {
                        throw new ParamException("Vector field dimension must be larger than zero");
                    }
                } catch (NumberFormatException e) {
                    throw new ParamException("Vector field dimension must be an integer number");
                }

                if (!typeParams.containsKey(Constant.BATCH_NORMALIZE)) {
                    typeParams.put(Constant.BATCH_NORMALIZE, Boolean.toString(false));
                }
            }

            if (dataType == DataType.String) {
                if (!typeParams.containsKey(Constant.MAX_LENGTH)) {
                    throw new ParamException("String field max length must be specified");
                }

                try {
                    int len = Integer.parseInt(typeParams.get(Constant.MAX_LENGTH));
                    if (len <= 0) {
                        throw new ParamException("String field max length must be larger than zero");
                    }
                } catch (NumberFormatException e) {
                    throw new ParamException("String field max length must be an integer number");
                }

                if (!typeParams.containsKey(Constant.BATCH_NORMALIZE)) {
                    typeParams.put(Constant.BATCH_NORMALIZE, Boolean.toString(false));
                }
            }

            return new FieldType(this);
        }
    }

    /**
     * Construct a <code>String</code> by {@link FieldType} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        return "FieldType{" +
                "name='" + name + '\'' +
                ", type='" + dataType.name() + '\'' +
                ", params=" + typeParams +
                '}';
    }
}
