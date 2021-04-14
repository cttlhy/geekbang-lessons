package org.geektimes.cache.lettuce;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.geektimes.cache.AbstractCache;
import org.geektimes.cache.ExpirableEntry;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LettuceCache<K extends Serializable, V extends Serializable> extends AbstractCache<K, V> {

    private final StatefulRedisConnection<K,V> connection;
    private final RedisCommands<K,V> syncCommands;

    public LettuceCache(CacheManager cacheManager, String cacheName,
                        Configuration<K, V> configuration, StatefulRedisConnection<K, V> connection) {
        super(cacheManager, cacheName, configuration);
        this.connection = connection;
        this.syncCommands = connection.sync();
    }

    @Override
    protected boolean containsEntry(K key) throws CacheException, ClassCastException {
        return syncCommands.exists(key) > 0;
    }

    @Override
    protected ExpirableEntry<K, V> getEntry(K key) throws CacheException, ClassCastException {
        return ExpirableEntry.of(key, syncCommands.get(key));
    }


    @Override
    protected void putEntry(ExpirableEntry<K, V> entry) throws CacheException, ClassCastException {
        syncCommands.set(entry.getKey(),entry.getValue());
    }

    @Override
    protected ExpirableEntry<K, V> removeEntry(K key) throws CacheException, ClassCastException {
        ExpirableEntry<K, V> oldEntry = getEntry(key);
        syncCommands.del(key);
        return oldEntry;
    }

    @Override
    protected void clearEntries() throws CacheException {
        // TODO
        Set<K> ks = keySet();
        ks.forEach(this::removeEntry);
    }


    @Override
    protected Set<K> keySet() {
        KeyScanCursor<K> scan = syncCommands.scan();
        List<K> keys = scan.getKeys();
        return  new HashSet<>(keys);
    }

    @Override
    protected void doClose() {
        connection.close();
    }
}
