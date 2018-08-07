package com.redislabs.redisgraph;
import redis.clients.jedis.util.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;


public class Commands {
    public enum Command implements ProtocolCommand {
        QUERY("graph.QUERY");

        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }
}