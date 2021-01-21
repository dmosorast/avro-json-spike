package custom_logical_type;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class JsonLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    private final LogicalType jsonLogicalType = new JsonLogicalType();
    @Override
    public LogicalType fromSchema(Schema schema) {
        return jsonLogicalType;
    }
}
