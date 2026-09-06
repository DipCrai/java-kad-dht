package com.libp2p.kademlia.refresh;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pulls live kad peer addresses from public libp2p <em>Delegated Routing V1</em>
 * HTTP servers ({@code GET /routing/v1/dht/closest/peers/{key}}), used as an
 * automatic bootstrap fallback when the configured real bootstrap peers do not
 * work (e.g. unreachable, IP-blocked or silent on the kad protocol from the
 * caller's network).
 *
 * <p>The HTTP routers resolve the kad lookup themselves and answer in a fraction
 * of a second over plain HTTPS. This source is only meant to obtain the
 * <em>first</em> addresses: once its peers are dialed and inserted into the
 * routing table, normal kad lookups take over, keep the table alive and growing,
 * and no further HTTP requests are needed.</p>
 *
 * <p>The returned candidates carry <em>all</em> addresses advertised by the
 * router, verbatim, regardless of transport (tcp, quic, udp, webrtc, dns, ws,
 * ...), each suffixed with its peer id. Transport suitability is decided by the
 * caller (the host's dialer): attempting to dial an unsupported transport fails
 * gracefully and is skipped, so no transport is thrown away at parsing time. A
 * per-instance {@link Predicate} can restrict the candidate set when desired.</p>
 *
 * <p>No JSON dependency is pulled in: the response schema
 * ({@code {"Peers":[{"Addrs":[...],"ID":"...","Schema":"peer"},...]}}) is stable
 * and parsed with a small dedicated extractor.</p>
 *
 * @see <a href="https://specs.ipfs.tech/routing/http-routing-v1/">HTTP Routing V1 spec</a>
 */
public class HttpBootstrapPeerSource implements PeerSource {

    public static final String DEFAULT_ROUTER = "https://delegated-ipfs.dev/routing/v1";
    public static final String DEFAULT_ROUTER_FALLBACK = "https://cid.contact/routing/v1";

    /** Default public Delegated Routing V1 servers; queried in order, failures are skipped. */
    public static final List<String> DEFAULT_ROUTERS = List.of(DEFAULT_ROUTER, DEFAULT_ROUTER_FALLBACK);

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final int DEFAULT_MAX_PEERS = 50;
    private static final int DEFAULT_LOOKUP_KEYS = 3;
    private static final int DEFAULT_DIAL_LIMIT = 50;

    private static final String BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";
    private static final String CIDV1_PREFIX = "b"; // multibase base32-lower-no-pad

    private static final Pattern ADDRS_PATTERN = Pattern.compile("\"Addrs\"\\s*:\\s*\\[([^\\]]*)]");
    private static final Pattern ID_PATTERN = Pattern.compile("\"ID\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpClient client;
    private final List<String> routers;
    private final Duration requestTimeout;
    private final int maxPeers;
    private final int lookupKeys;
    private final Predicate<String> addrFilter;
    private static final SecureRandom random = new SecureRandom();

    public HttpBootstrapPeerSource(List<String> routers, Duration requestTimeout, int maxPeers, int lookupKeys) {
        this(routers, requestTimeout, maxPeers, lookupKeys, defaultAddrFilter());
    }

    /**
     * @param addrFilter decides which raw multiaddr strings from the router are
     *                   kept as candidates; default keeps everything, transport
     *                   suitability is left to the caller's dialer
     */
    public HttpBootstrapPeerSource(List<String> routers, Duration requestTimeout, int maxPeers, int lookupKeys, Predicate<String> addrFilter) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.routers = new ArrayList<>(routers);
        this.requestTimeout = requestTimeout;
        this.maxPeers = maxPeers;
        this.lookupKeys = Math.max(1, lookupKeys);
        this.addrFilter = addrFilter != null ? addrFilter : defaultAddrFilter();
    }

    /** Constructs a source with the default public routers and timing. */
    public static HttpBootstrapPeerSource defaults() {
        return new HttpBootstrapPeerSource(DEFAULT_ROUTERS, DEFAULT_REQUEST_TIMEOUT, DEFAULT_MAX_PEERS, DEFAULT_LOOKUP_KEYS);
    }

    /** @return the number of lookup keys queried per router */
    public int getLookupKeys() { return lookupKeys; }

    /** @return a filter accepting any address; use it to enable every transport */
    public static Predicate<String> defaultAddrFilter() {
        return a -> true;
    }

    /** @return a typical dialable-only filter (ip4/ip6 + tcp/quic/quic-v1) for hosts without exotic transports */
    public static Predicate<String> ipTcpQuicAddrFilter() {
        return a -> {
            if (a == null) return false;
            String addr = a.replaceAll("/p2p/.*", "");
            if (!addr.startsWith("/ip4/") && !addr.startsWith("/ip6/")) return false;
            if (!addr.contains("/tcp/") && !addr.contains("/quic")) return false;
            return !addr.contains("webrtc") && !addr.contains("webtransport");
        };
    }

    /**
     * Queries the configured routers for kad peers close to {@code keyCount}
     * lookup keys (the local peer id plus fresh random keys), deduplicates exact
     * addresses and returns up to {@link #maxPeers} candidate multiaddrs, each
     * suffixed with its peer id. Any failure (network, 4xx/5xx) yields an empty
     * result without throwing.
     */
    public CompletableFuture<List<Multiaddr>> fetch(PeerId self, int keyCount) {
        List<String> keys = buildKeys(self, keyCount);
        return fetchKeys(keys);
    }

    /**
     * Fetches the responsables (closest/peers) for one specific DHT content key,
     * deterministically derived (same key on every caller), so two parties can
     * coordinate on the same set of peers near that key.
     */
    public CompletableFuture<List<Multiaddr>> fetchForDhtKey(byte[] dhtKey) {
        return fetchKeys(List.of(dhtKeyFor(dhtKey)));
    }

    /** Deterministic routing key (base32 cidv1-raw) for an arbitrary content key. */
    public static String dhtKeyFor(byte[] contentKey) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return newCidKey(md.digest(contentKey));
        } catch (Exception e) {
            return newRandomCidKey();
        }
    }

    /** Fetches, deduplicates and caps the candidates for the given routing keys. */
    public CompletableFuture<List<Multiaddr>> fetchKeys(List<String> keys) {
        List<CompletableFuture<List<Multiaddr>>> fetches = new ArrayList<>();
        for (String router : routers) {
            for (String key : keys) {
                fetches.add(fetchRouter(router, key));
            }
        }
        return CompletableFuture.allOf(fetches.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    Set<String> seenAddrs = new LinkedHashSet<>();
                    List<Multiaddr> out = new ArrayList<>();
                    for (CompletableFuture<List<Multiaddr>> f : fetches) {
                        for (Multiaddr m : f.join()) {
                            if (out.size() >= maxPeers) return out;
                            if (seenAddrs.add(m.toString())) out.add(m);
                        }
                    }
                    return out;
                });
    }

    private CompletableFuture<List<Multiaddr>> fetchRouter(String router, String key) {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(router + "/dht/closest/peers/" + key))
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        return client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                .thenApply(resp -> resp.statusCode() == 200 ? parsePeers(resp.body(), addrFilter) : List.<Multiaddr>of())
                .exceptionally(ex -> List.<Multiaddr>of());
    }

    /**
     * Best-effort extraction of the Routing V1 {@code closest/peers} response
     * body. Every advertised address of every peer is kept verbatim (no transport
     * filtering), each combined with its reported peer id.
     */
    static List<Multiaddr> parsePeers(String json) {
        return parsePeers(json, defaultAddrFilter());
    }

    /** Variant of {@link #parsePeers(String)} honoring a custom addr filter. */
    static List<Multiaddr> parsePeers(String json, Predicate<String> addrFilter) {
        List<Multiaddr> out = new ArrayList<>();
        for (String obj : extractObjects(json)) {
            Matcher idM = ID_PATTERN.matcher(obj);
            if (!idM.find()) continue;
            String id = idM.group(1);
            Matcher addrsM = ADDRS_PATTERN.matcher(obj);
            if (!addrsM.find()) continue;
            for (String addr : splitStringList(addrsM.group(1))) {
                if (!addrFilter.test(addr)) continue;
                try {
                    out.add(Multiaddr.fromString(addr + "/p2p/" + id));
                } catch (Exception ignored) {
                }
            }
        }
        return out;
    }

    /** Extracts the top-level objects of the {@code Peers} array. */
    static List<String> extractObjects(String json) {
        List<String> out = new ArrayList<>();
        if (json == null) return out;
        int arr = json.indexOf("\"Peers\"");
        if (arr < 0) return out;
        int start = json.indexOf('[', arr);
        if (start < 0) return out;
        int depth = 0;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{') {
                if (depth == 0) start = i;
                depth++;
            } else if (c == '}' && depth > 0) {
                depth--;
                if (depth == 0) out.add(json.substring(start, i + 1));
            }
            // array brackets are ignored: only braces delimit peer objects, and a
            // value array never opens a nested object
        }
        return out;
    }

    /** Splits a JSON array of strings, honoring escaped quotes inside items. */
    static List<String> splitStringList(String arrayBody) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean in = false, esc = false;
        for (int i = 0; i < arrayBody.length(); i++) {
            char c = arrayBody.charAt(i);
            if (in) {
                if (esc) { cur.append(c); esc = false; }
                else if (c == '\\') { esc = true; }
                else if (c == '"') { in = false; out.add(cur.toString()); cur.setLength(0); }
                else cur.append(c);
            } else if (c == '"') {
                in = true;
            }
        }
        return out;
    }

    /** Builds the lookup keys: the local peer id (ed25519 or rsa both work as
     * an HTTP routing key) plus fresh random CIDv1-raw keys that land in
     * different buckets of the DHT spread. */
    static List<String> buildKeys(PeerId self, int keyCount) {
        List<String> keys = new ArrayList<>();
        if (self != null) {
            keys.add(self.toBase58());
        }
        while (keys.size() < keyCount) {
            keys.add(newRandomCidKey());
        }
        return keys;
    }

    /** Fresh random CIDv1 raw (codec 0x55) wrapping a sha2-256 multihash. */
    static String newRandomCidKey() {
        byte[] rand = new byte[32];
        random.nextBytes(rand);
        return newCidKey(rand);
    }

    /** CIDv1 raw (codec 0x55) wrapping a sha2-256 multihash of the given digest. */
    static String newCidKey(byte[] digest) {
        byte[] payload = new byte[4 + digest.length];
        payload[0] = 0x01; // cid v1
        payload[1] = 0x55; // raw codec
        payload[2] = 0x12; // sha2-256
        payload[3] = (byte) digest.length;
        System.arraycopy(digest, 0, payload, 4, digest.length);
        return CIDV1_PREFIX + base32Lower(payload);
    }

    static String base32Lower(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        int buffer = 0, bits = 0;
        for (byte b : bytes) {
            buffer = (buffer << 8) | (b & 0xff);
            bits += 8;
            while (bits >= 5) {
                sb.append(BASE32_ALPHABET.charAt((buffer >> (bits - 5)) & 0x1f));
                bits -= 5;
            }
        }
        if (bits > 0) {
            sb.append(BASE32_ALPHABET.charAt((buffer << (5 - bits)) & 0x1f));
        }
        return sb.toString();
    }
}