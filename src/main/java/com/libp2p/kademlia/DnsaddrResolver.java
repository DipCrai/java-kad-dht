package com.libp2p.kademlia;

import io.libp2p.core.multiformats.Multiaddr;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Iterative multiaddr DNS resolver, ported from go-multiaddr-dns.
 *
 * <p>Resolves {@code /dnsaddr/...} entries by looking up the {@code _dnsaddr.<domain>}
 * TXT record, keeping records whose {@code dnsaddr=} value ends with the requested
 * suffix (e.g. {@code /p2p/<peerId>}), then resolving {@code /dns4/}, {@code /dns6/}
 * and {@code /dns/} hosts via A/AAAA lookups down to concrete {@code /ip4|/ip6}
 * multiaddrs. This is the same bootstrap mechanism go-libp2p uses for its default
 * peers (e.g. /dnsaddr/bootstrap.libp2p.io/...).
 */
public final class DnsaddrResolver {

    private static final String DNSADDR_PROTO = "/dnsaddr/";
    private static final String[] DNS_PROTOS = {"/dns4/", "/dns6/", "/dns/"};
    private static final int MAX_HOPS = 16;

    private DnsaddrResolver() {}

    /** Fully resolves a (possibly DNS-based) multiaddr string into concrete ip-based multiaddrs. */
    public static List<Multiaddr> resolve(String addr) {
        Set<String> out = new LinkedHashSet<>();
        expand(addr, out, 0);
        List<Multiaddr> res = new ArrayList<>();
        for (String s : out) {
            try {
                res.add(Multiaddr.fromString(s));
            } catch (Exception ignored) {
            }
        }
        return res;
    }

    /** Returns true if the given multiaddr string contains a DNS protocol that requires resolution. */
    public static boolean isResolvable(String addr) {
        return addr != null && (addr.contains(DNSADDR_PROTO) || containsDnsProto(addr));
    }

    private static void expand(String ma, Set<String> out, int depth) {
        if (depth > MAX_HOPS || ma == null) {
            return;
        }
        String s = ma.trim();
        if (s.isEmpty()) {
            return;
        }
        int di = s.indexOf(DNSADDR_PROTO);
        if (di >= 0) {
            int hostStart = di + DNSADDR_PROTO.length();
            int slash = s.indexOf('/', hostStart);
            String domain = slash < 0 ? s.substring(hostStart) : s.substring(hostStart, slash);
            String post = slash < 0 ? "" : s.substring(slash);
            for (String rec : lookupTxt("_dnsaddr." + domain)) {
                if (!rec.startsWith("dnsaddr=")) {
                    continue;
                }
                String cand = rec.substring("dnsaddr=".length());
                if (!post.isEmpty() && !cand.endsWith(post)) {
                    continue;
                }
                expand(cand, out, depth + 1);
            }
            return;
        }
        for (String proto : DNS_PROTOS) {
            int idx = s.indexOf(proto);
            if (idx < 0) {
                continue;
            }
            String head = s.substring(0, idx);
            String rest = s.substring(idx + proto.length());
            int hostEnd = rest.indexOf('/');
            String host = hostEnd < 0 ? rest : rest.substring(0, hostEnd);
            String tail = hostEnd < 0 ? "" : rest.substring(hostEnd);
            try {
                for (InetAddress ip : InetAddress.getAllByName(host)) {
                    String ipStr = ip.getHostAddress();
                    String ipProto = ipStr.contains(":") ? "/ip6/" : "/ip4/";
                    expand(head + ipProto + ipStr + tail, out, depth + 1);
                }
            } catch (Exception ignored) {
            }
            return;
        }
        out.add(s);
    }

    private static boolean containsDnsProto(String s) {
        for (String p : DNS_PROTOS) {
            if (s.contains(p)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> lookupTxt(String name) {
        List<String> res = new ArrayList<>();
        try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
            env.put("java.naming.provider.url", "dns:");
            DirContext ctx = new InitialDirContext(env);
            Attributes attrs = ctx.getAttributes(name, new String[]{"TXT"});
            Attribute txt = attrs.get("TXT");
            if (txt != null) {
                NamingEnumeration<?> all = txt.getAll();
                while (all.hasMore()) {
                    Object v = all.next();
                    res.add(String.valueOf(v));
                }
            }
        } catch (Exception ignored) {
        }
        return res;
    }
}