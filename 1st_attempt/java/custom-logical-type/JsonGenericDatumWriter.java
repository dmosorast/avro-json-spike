// HACK: This is a subclass to override the default handling of this, so
// that we can support generic Object types being returned to and from a
// custom logical type
// - NB This will likely need an overridden reader as well
// Due to: https://github.com/apache/avro/blob/release-1.9.2/lang/java/avro/src/main/java/org/apache/avro/generic/GenericDatumWriter.java#L79

package custom_logical_type;

import java.io.IOException;

import org.apache.avro.LogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;

public class JsonGenericDatumWriter<D> extends GenericDatumWriter<D> {
    public JsonGenericDatumWriter() {
        super();
    }

    protected JsonGenericDatumWriter(GenericData data) {
        super(data);
    }

    public JsonGenericDatumWriter(Schema root) {
        super(root);
    }

    public JsonGenericDatumWriter(Schema root, GenericData data) {
        super(root, data);
    }

    @Override
    protected void write(Schema schema, Object datum, Encoder out) throws IOException {
        LogicalType logicalType = schema.getLogicalType();
        // TODO: This could probably just check if ANYONE has registered an Object conversion as a last resort.
        // - If this would be a "supported" pattern.
        if (logicalType instanceof JsonLogicalType) {
            // PoC (this probably could be better)
            // BUT Use Object for Json logical Types
            Conversion<?> conversion = getData().getConversionByClass(Object.class, logicalType);
            writeWithoutConversion(schema, convert(schema, logicalType, conversion, datum), out);
        } else if (datum != null && logicalType != null) {
            Conversion<?> conversion = getData().getConversionByClass(datum.getClass(), logicalType);
            writeWithoutConversion(schema, convert(schema, logicalType, conversion, datum), out);
        } else {
            writeWithoutConversion(schema, datum, out);
        }
    }
}
