package org.geektimes.cache.serializer;

import javax.cache.CacheException;
import java.io.*;

/**
 * @author ctt
 */
public class CacheSerializerImp<S,T> implements CacheSerializer<S,T> {

    @Override
    public byte[] serialize(Object value) throws CacheException {
        byte[] bytes = null;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)
        ) {
            // Key -> byte[]
            objectOutputStream.writeObject(value);
            bytes = outputStream.toByteArray();
        } catch (IOException e) {
            throw new CacheException(e);
        }
        return bytes;
    }


    @Override
    public T deserialize(byte[] bytes) throws CacheException {
        if (bytes == null) {
            return null;
        }
        T value = null;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)
        ) {
            // byte[] -> Value
            value = (T) objectInputStream.readObject();
        } catch (Exception e) {
            throw new CacheException(e);
        }
        return value;
    }
}
