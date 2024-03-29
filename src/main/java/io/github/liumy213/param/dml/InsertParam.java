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
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * Parameters for <code>insert</code> interface.
 */
@Getter
public class InsertParam {
    protected final List<Field> fields;
    protected final String collectionName;
    protected final String partitionName;
    protected final int rowCount;

    protected InsertParam(@NonNull Builder builder) {
        this.collectionName = builder.collectionName;
        this.partitionName = builder.partitionName;
        this.fields = builder.fields;
        this.rowCount = builder.rowCount;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link InsertParam} class.
     */
    public static class Builder {
        protected String databaseName;
        protected String collectionName;
        protected String partitionName = "";
        protected List<Field> fields;
        protected int rowCount;

        protected Builder() {
        }

        /**
         * Sets the database name. database name can be nil.
         *
         * @param databaseName database name
         * @return <code>Builder</code>
         */
        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
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
         * Set partition name (Optional).
         * This partition name will be ignored if the collection has a partition key field.
         *
         * @param partitionName partition name
         * @return <code>Builder</code>
         */
        public Builder withPartitionName(@NonNull String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        /**
         * Sets the column data to insert. The field list cannot be empty.
         *
         * @param fields insert column data
         * @return <code>Builder</code>
         * @see Field
         */
        public Builder withFields(@NonNull List<Field> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * Verifies parameters and creates a new {@link InsertParam} instance.
         *
         * @return {@link InsertParam}
         */
        public InsertParam build() throws ParamException {
            ParamUtils.CheckNullEmptyString(collectionName, "Collection name");

            if (CollectionUtils.isEmpty(fields)) {
                throw new ParamException("Fields cannot be empty");
            }

            int count = 0;
            if (CollectionUtils.isNotEmpty(fields)) {
                if (fields.get(0) == null) {
                    throw new ParamException("Field cannot be null." +
                            " If the field is auto-id, just ignore it from withFields()");
                }
                count = fields.get(0).getValues().size();
                checkFields(count);
            }

            this.rowCount = count;

            if (count == 0) {
                throw new ParamException("Zero row count is not allowed");
            }

            // this method doesn't check data type, the insert() api will do this work
            return new InsertParam(this);
        }

        protected void checkFields(int count) {
            for (Field field : fields) {
                if (field == null) {
                    throw new ParamException("Field cannot be null." +
                            " If the field is auto-id, just ignore it from withFields()");
                }

                ParamUtils.CheckNullEmptyString(field.getName(), "Field name");

                if (field.getValues() == null || field.getValues().isEmpty()) {
                    throw new ParamException("Field value cannot be empty." +
                            " If the field is auto-id, just ignore it from withFields()");
                }
            }

            // check row count
            for (Field field : fields) {
                if (field.getValues().size() != count) {
                    throw new ParamException("Row count of fields must be equal");
                }
            }
        }
    }

    /**
     * Constructs a <code>String</code> by {@link InsertParam} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        String baseStr = "InsertParam{" +
                "collectionName='" + collectionName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", rowCount=" + rowCount;
        if (!CollectionUtils.isEmpty(fields)) {
            return baseStr +
                    ", columnFields+" + fields +
                    '}';
        } else {
            return baseStr + '}';
        }
    }

    /**
     * Internal class for insert data.
     * If dataType is Bool, values is List of Boolean;
     * If dataType is Int64, values is List of Long;
     * If dataType is Float, values is List of Float;
     * If dataType is Double, values is List of Double;
     * If dataType is Varchar, values is List of String;
     * If dataType is FloatVector, values is List of List Float;
     *
     */
    @lombok.Builder
    public static class Field {
        private final String name;
        private final List<?> values;

        public Field(String name, List<?> values) {
            this.name = name;
            this.values = values;
        }

        /**
         * Return name of the field.
         *
         * @return <code>String</code>
         */
        public String getName() {
            return name;
        }

        /**
         * Return data of the field, in column-base.
         *
         * @return <code>List</code>
         */
        public List<?> getValues() {
            return values;
        }

        /**
         * Constructs a <code>String</code> by {@link Field} instance.
         *
         * @return <code>String</code>
         */
        @Override
        public String toString() {
            return "Field{" +
                    "fieldName='" + name + '\'' +
                    ", row_count=" + values.size() +
                    '}';
        }
    }
}
