package custom_logical_type;

import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public abstract class JsonConversion<T> extends Conversion<T> {
    // Construct a unique instance for all of the conversions.
    public static final List<JsonConversion> INSTANCES =
        List.of(new ConcreteJsonConversions.PersistentArrayMapConversion(),
                new ConcreteJsonConversions.PersistentVectorConversion());

    private IFn generateString = Clojure.var("cheshire.core", "generate-string");
    private IFn parseString = Clojure.var("cheshire.core", "parse-string");

    public JsonConversion() { super(); }

    @Override
    public abstract Class<T> getConvertedType();

    @Override
    public String getLogicalTypeName() { return JsonLogicalType.JSON_LOGICAL_TYPE_NAME; }

    public T fromCharSequence(CharSequence value) {
        return fromCharSequence(value, null, null);
    }

    // TODO: This need performance tested
    @Override
    public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return (T)parseString.invoke(value.toString());
    }

    public CharSequence toCharSequence(T value) {
        return toCharSequence(value, null, null);
    }

    @Override
    public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
        return (CharSequence)generateString.invoke(value);
    }
}
