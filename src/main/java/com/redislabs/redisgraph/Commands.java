package com.redislabs.redisgraph;
import redis.clients.util.SafeEncoder;
import redis.clients.jedis.commands.ProtocolCommand;


public class Commands {
    public enum Command implements ProtocolCommand {
        CREATENODE("graph.CREATENODE"),
        ADDEDGE("graph.ADDEDGE"),
        GETEDGE("graph.GETEDGE"),
        GETNODE("graph.GETNODE"),
        GETNODEEDGES("graph.GETNODEEDGES"),
        GETNEIGHBOURS("graph.GETNEIGHBOURS"),
        REMOVEEDGE("graph.REMOVEEDGE"),
        DELETEGRAPH("graph.DELETE"),
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