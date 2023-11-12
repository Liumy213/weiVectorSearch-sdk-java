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
import io.github.liumy213.param.ParamUtils;
import io.github.liumy213.rpc.DataType;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Parameters for <code>createCollection</code> interface.
 */
@Getter
@ToString
public class CreateCollectionParam {
    private final String collectionName;
    private final String description;
    private final List<FieldType> fieldTypes;

    private CreateCollectionParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.description = builder.description;
        this.fieldTypes = builder.fieldTypes;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link CreateCollectionParam} class.
     */
    public static final class Builder {
        private String collectionName;
        private String description = "";
        private final List<FieldType> fieldTypes = new ArrayList<>();
        private Builder() {
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
         * Sets the collection description. The description can be empty. The default is "".
         *
         * @param description description of the collection
         * @return <code>Builder</code>
         */
        public Builder withDescription(@NonNull String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the schema of the collection. The schema cannot be empty or null.
         * @see FieldType
         *
         * @param fieldTypes a <code>List</code> of {@link FieldType}
         * @return <code>Builder</code>
         */
        public Builder withFieldTypes(@NonNull List<FieldType> fieldTypes) {
            this.fieldTypes.addAll(fieldTypes);
            return this;
        }

        /**
         * Adds a field schema.
         * @see FieldType
         *
         * @param fieldType a {@link FieldType} object
         * @return <code>Builder</code>
         */
        public Builder addFieldType(@NonNull FieldType fieldType) {
            this.fieldTypes.add(fieldType);
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link CreateCollectionParam} instance.
         *
         * @return {@link CreateCollectionParam}
         */
        public CreateCollectionParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (fieldTypes.isEmpty()) {
                throw new ParamException("Field numbers must be larger than 0");
            }

            boolean hasVectorField = false;
            boolean hasStringField = false;
            for (FieldType fieldType : fieldTypes) {
                if (fieldType == null) {
                    throw new ParamException("Collection field cannot be null");
                }

                if (fieldType.getDataType() == DataType.FloatVector) {
                    hasVectorField = true;
                }

                if (fieldType.getDataType() == DataType.String) {
                    hasStringField = true;
                }

                if (hasStringField && hasVectorField) {
                    throw new ParamException("FloatVector and String field can not be same time to create, only one field is allowed in a collection");
                }
            }

            return new CreateCollectionParam(this);
        }
    }
}
