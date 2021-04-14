package org.geektimes.cache.serializer;

import javax.cache.CacheException;

public interface CacheSerializer<S,T> {

    byte[] serialize(S source) throws CacheException;

    T deserialize(byte[] bytes) throws CacheException;
}
