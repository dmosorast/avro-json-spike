package custom_logical_type;

import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.function.Function;

import org.apache.avro.LogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class JsonConversion extends Conversion<Object> {
    // Construct a unique instance for all the conversion. This have to be changed in case the conversion
    //   needs some runtime information (e.g.: an encryption key / a tenant_ID). If so, the get() method should
    //   return the appropriate conversion per key.
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonConversion INSTANCE = new JsonConversion();
    public static final JsonConversion get(){ return INSTANCE; }
    private JsonConversion(){ super(); }

    @Override
    public Class<Object> getConvertedType() { return Object.class; }

    @Override
    public String getLogicalTypeName() { return JsonLogicalType.JSON_LOGICAL_TYPE_NAME; }

    private Function<String,Object> _readerFunc = null;
    public void setJsonReader(Function<String,Object> readerFunc) {
        _readerFunc = readerFunc;
    }
    private Function<Object,String> _writerFunc = null;
    public void setJsonWriter(Function<Object,String> writerFunc) {
        _writerFunc = writerFunc;
    }

    public Object getJsonWriter() {
        return _writerFunc;
    }

    public Object fromCharSequence(CharSequence value) {
        return fromCharSequence(value, null, null);
    }

    @Override
    public Object fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        try {
            String strValue = value.toString();
            if (_readerFunc != null) {
                return _readerFunc.apply(strValue);
            }
            if (strValue.startsWith("{")) {
                return MAPPER.readValue(strValue, Map.class);
            } else if (strValue.startsWith("[")) {
                return MAPPER.readValue(strValue, ArrayList.class);
            } else {
                throw new RuntimeException("JsonConversion.fromCharSequence - failed to deserialize value: " + strValue);
            }
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public CharSequence toCharSequence(Object value) {
        return toCharSequence(value, null, null);
    }

    @Override
    public CharSequence toCharSequence(Object value, Schema schema, LogicalType type) {
        try {
            if (_writerFunc != null) {
                return _writerFunc.apply(value);
            }

            return MAPPER.writer().writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
