package com.libp2p.kademlia;

/**
 * Validates and selects DHT records.
 * Port of go-libp2p record.Validator and rust RecordStore validation.
 *
 * Implementations:
 * - PublicKeyValidator: validates /pk/* keys (value must be valid pub key)
 * - IpnsValidator: validates /ipns/* keys (if IPNS support needed)
 * - NamespacedValidator: routes by key namespace prefix
 * - NoopValidator: accepts everything (for custom DHT use)
 */
public interface RecordValidator {

    /**
     * Validate a record. Returns true if the record is acceptable.
     */
    boolean validate(byte[] key, byte[] value);

    /**
     * Select the best record from multiple candidates for the same key.
     * Returns the index of the best value, or -1 if none are acceptable.
     */
    int select(byte[] key, byte[][] values);

    /**
     * Validator for public key records under /pk/* namespace.
     */
    RecordValidator PUBLIC_KEY = new RecordValidator() {
        @Override
        public boolean validate(byte[] key, byte[] value) {
            return value != null && value.length > 0;
        }

        @Override
        public int select(byte[] key, byte[][] values) {
            if (values == null || values.length == 0) return -1;
            return 0;
        }
    };

    /**
     * Accepts everything. For custom DHT use cases where the application handles validation.
     */
    RecordValidator NOOP = new RecordValidator() {
        @Override
        public boolean validate(byte[] key, byte[] value) {
            return value != null && value.length > 0;
        }

        @Override
        public int select(byte[] key, byte[][] values) {
            if (values == null || values.length == 0) return -1;
            return 0;
        }
    };
}
