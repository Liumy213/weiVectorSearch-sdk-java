package io.github.liumy213.response;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.liumy213.exception.ParamException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowRecord {
    Map<String, Object> fieldValues = new HashMap<>();

    public RowRecord() {}

    public boolean put(String keyName, Object obj) {
        if (fieldValues.containsKey(keyName)) {
            return false;
        }
        fieldValues.put(keyName, obj);

        return true;
    }

    /**
     * Get a value by a key name. If the key name is a field name, return the value of this field.
     * If the key name is in dynamic field, return the value from the dynamic field.
     * Throws {@link ParamException} if the key name doesn't exist.
     *
     * @return {@link FieldDataWrapper}
     */
    public Object get(String keyName) throws ParamException {
        if (fieldValues.isEmpty()) {
            throw new ParamException("This record is empty");
        }

        Object obj = fieldValues.get(keyName);
        if (obj == null) {
            // find the value from dynamic field
            Object meta = fieldValues.get("$meta");
            if (meta != null) {
                JsonNode jsonMata = (JsonNode) meta;
                Object innerObj = jsonMata.get(keyName);
                if (innerObj != null) {
                    return innerObj;
                }
            }
            throw new ParamException("The key name is not found");
        }

        return obj;
    }

    /**
     * Constructs a <code>String</code> by {@link RowRecord} instance.
     *
     * @return <code>String</code>
     */
    @Override
    public String toString() {
        List<String> pairs = new ArrayList<>();
        fieldValues.forEach((keyName, fieldValue) -> {
            pairs.add(keyName + ":" + fieldValue.toString());
        });
        return pairs.toString();
    }
}
