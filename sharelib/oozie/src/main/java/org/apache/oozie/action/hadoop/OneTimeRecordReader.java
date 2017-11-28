/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.oozie.action.hadoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * A {@link RecordReader} that reads exactly once. When {@link #next(Writable, Writable)} is called the second time, it notifies the
 * caller that there are no more items to read.
 *
 * <p>The actual implementation doesn't perform any reading, it just counts the times it was called, and responds w/ dummy output.
 *
 * @param <K> the key type. Must be a {@link Writable}
 * @param <V> the value type. Must be a {@link Writable}
 */
class OneTimeRecordReader<K extends Writable, V extends Writable> implements RecordReader<K, V> {
    private final Class<K> keyWritableClass;
    private final Class<V> valueWritableClass;

    private boolean isReadingDone = false;

    OneTimeRecordReader(final Class<K> keyWritableClass,
                        final Class<V> valueWritableClass) {
        this.keyWritableClass = keyWritableClass;
        this.valueWritableClass = valueWritableClass;
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    @Override
    public float getProgress() throws IOException {
        if (isReadingDone) {
            return 1.0f;
        } else
            return 0.0f;
    }

    @Override
    public K createKey() {
        try {
            return keyWritableClass.getDeclaredConstructor().newInstance();
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException(
                    String.format("Cannot create key. An error while instantiating key writable class %s",
                            keyWritableClass.getName()),
                    e);
        }
    }

    @Override
    public V createValue() {
        try {
            return valueWritableClass.getDeclaredConstructor().newInstance();
        } catch (final InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(
                    String.format("Cannot create value. An error while instantiating value writable class %s",
                            keyWritableClass.getName()),
                    e);
        }
    }

    @Override
    public long getPos() throws IOException {
        if (isReadingDone) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean next(final K key, final V value) throws IOException {
        if (isReadingDone) {
            return false;
        } else {
            isReadingDone = true;
            return true;
        }
    }
}
