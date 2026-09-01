package com.libp2p.kademlia.records;

/**
 * Validator for DHT records.
 *
 * <p>Used to filter invalid records during GET_VALUE and to select the best
 * record when multiple valid records are returned.</p>
 *
 * <h3>Built-in validators</h3>
 * <ul>
 *   <li>{@link #NOOP} — accepts any non-empty value, selects first</li>
 *   <li>{@link #PUBLIC_KEY} — alias for NOOP (Go validates /pk keys via protobuf)</li>
 * </ul>
 *
 * <h3>Custom validators</h3>
 * <p>Implement this interface and pass via {@link com.libp2p.kademlia.config.KadConfig.Builder#validator(RecordValidator)}.
 * Namespaced validators ({@link NamespacedValidator}) route by key prefix.</p>
 */
public interface RecordValidator {

    /**
     * Validate whether a key-value pair is acceptable.
     *
     * @param key   the record key
     * @param value the record value
     * @return true if the record is valid
     */
    boolean validate(byte[] key, byte[] value);

    /**
     * Select the best record from multiple valid records.
     *
     * @param key    the record key
     * @param values the candidate values
     * @return index of the selected value, or -1 if none selected
     */
    int select(byte[] key, byte[][] values);

    /** No-op validator: accepts any non-empty value. */
    RecordValidator NOOP = new RecordValidator() {
        @Override public boolean validate(byte[] key, byte[] value) { return value != null && value.length > 0; }
        @Override public int select(byte[] key, byte[][] values) { return (values != null && values.length > 0) ? 0 : -1; }
    };

    /** Alias for NOOP. Go validates /pk keys via protobuf decoding. */
    RecordValidator PUBLIC_KEY = NOOP;
}
