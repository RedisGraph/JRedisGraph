package com.falkordb.impl.api;
import redis.clients.jedis.util.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * 
 */
public enum GraphCommand implements ProtocolCommand {
    QUERY("graph.QUERY"),
    RO_QUERY("graph.RO_QUERY"),
    DELETE("graph.DELETE");

    private final byte[] raw;

    GraphCommand(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
