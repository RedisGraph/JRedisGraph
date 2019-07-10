package com.redislabs.redisgraph.impl.api;
import redis.clients.jedis.util.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * 
 * 
 *
 */
public enum RedisGraphCommand implements ProtocolCommand {
    QUERY("graph.QUERY"),
    DELETE("graph.DELETE");

    private final byte[] raw;

    RedisGraphCommand(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
