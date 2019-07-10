package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.impl.graph_cache.RedisGraphCaches;

public interface RedisGraphCacheHolder {

    void setRedisGraphCaches(RedisGraphCaches caches);
}
