package custom_logical_type;

import java.util.List;

// Note, add all you need here, and implement them.
import clojure.lang.PersistentVector;
import clojure.lang.PersistentArrayMap;

public class ConcreteJsonConversions {
    // Convenience container class to extend the conversion for types that we need
    // - If cheshire doesn't handle the conversion out of the box, you can
    //   override `fromCharSequence` and `toCharSequence` in these classes
    public static class PersistentArrayMapConversion extends JsonConversion<PersistentArrayMap> {
        @Override
        public Class<PersistentArrayMap> getConvertedType() { return PersistentArrayMap.class; }
    }

    public static class PersistentVectorConversion extends JsonConversion<PersistentVector> {
        @Override
        public Class<PersistentVector> getConvertedType() { return PersistentVector.class; }
    }
}
