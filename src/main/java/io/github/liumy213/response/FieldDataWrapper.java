package io.github.liumy213.response;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.ProtocolStringList;
import io.github.liumy213.exception.IllegalResponseException;
import io.github.liumy213.exception.ParamException;
import io.github.liumy213.rpc.DataType;
import io.github.liumy213.rpc.FieldData;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;



/**
 * Utility class to wrap response of <code>query/search</code> interface.
 */
public class FieldDataWrapper {
    private final FieldData fieldData;

    public FieldDataWrapper(@NonNull FieldData fieldData) {
        this.fieldData = fieldData;
    }

    public boolean isVectorField() {
        return fieldData.getType() == DataType.FloatVector;
    }

    /**
     * Gets the dimension value of a vector field.
     * Throw {@link IllegalResponseException} if the field is not a vector filed.
     *
     * @return <code>int</code> dimension of the vector field
     */
    public int getDim() throws IllegalResponseException {
        if (!isVectorField()) {
            throw new IllegalResponseException("Not a vector field");
        }
        return (int) fieldData.getVectors().getDim();
    }

    /**
     * Gets the row count of a field.
     * * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>long</code> row count of the field
     */
    public long getRowCount() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned float vector field data array size doesn't match dimension");
                }

                return data.size()/dim;
            }
            case Int64:
                return fieldData.getScalars().getLongData().getDataList().size();
            case Int32:
                return fieldData.getScalars().getIntData().getDataList().size();
            case Bool:
                return fieldData.getScalars().getBoolData().getDataList().size();
            case Float:
                return fieldData.getScalars().getFloatData().getDataList().size();
            case Double:
                return fieldData.getScalars().getDoubleData().getDataList().size();
            case String:
                return fieldData.getScalars().getStringData().getDataList().size();
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    /**
     * Returns the field data according to its type:
     *      float vector field return List of List Float,
     *      binary vector field return List of ByteBuffer
     *      int64 field return List of Long
     *      int32 field return List of Integer
     *      boolean field return List of Boolean
     *      float field return List of Float
     *      double field return List of Double
     *      string field return List of String
     *      etc.
     *
     * Throws {@link IllegalResponseException} if the field type is illegal.
     *
     * @return <code>List</code>
     */
    public List<?> getFieldData() throws IllegalResponseException {
        DataType dt = fieldData.getType();
        switch (dt) {
            case FloatVector: {
                int dim = getDim();
                List<Float> data = fieldData.getVectors().getFloatVector().getDataList();
                if (data.size() % dim != 0) {
                    throw new IllegalResponseException("Returned float vector field data array size doesn't match dimension");
                }

                List<List<Float>> packData = new ArrayList<>();
                int count = data.size() / dim;
                for (int i = 0; i < count; ++i) {
                    packData.add(data.subList(i * dim, (i + 1) * dim));
                }
                return packData;
            }
            case Int64:
                return fieldData.getScalars().getLongData().getDataList();
            case Int32:
                return fieldData.getScalars().getIntData().getDataList();
            case Bool:
                return fieldData.getScalars().getBoolData().getDataList();
            case Float:
                return fieldData.getScalars().getFloatData().getDataList();
            case Double:
                return fieldData.getScalars().getDoubleData().getDataList();
            case String:
                ProtocolStringList protoStrList = fieldData.getScalars().getStringData().getDataList();
                return protoStrList.subList(0, protoStrList.size());
            default:
                throw new IllegalResponseException("Unsupported data type returned by FieldData");
        }
    }

    public Object valueByIdx(int index) throws ParamException {
        if (index < 0 || index >= getFieldData().size()) {
            throw new ParamException("index out of range");
        }
        return getFieldData().get(index);
    }

    private JSONObject parseObjectData(int index) {
        Object object = valueByIdx(index);
        return JSONObject.parseObject(new String((byte[])object));
    }
}
