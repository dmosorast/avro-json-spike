package custom_logical_type;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class JsonLogicalType extends LogicalType {
    //The key to use as a reference to the type
    public static final String JSON_LOGICAL_TYPE_NAME = "json";

    public JsonLogicalType() {
        super(JSON_LOGICAL_TYPE_NAME);
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        if (schema.getType() != Schema.Type.STRING) {
            throw new IllegalArgumentException(
                    "Logical type 'Json' must be backed by string");
        }
    }
}
