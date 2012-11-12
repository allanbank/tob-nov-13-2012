/*
 *         Copyright 2012 Allanbank Consulting, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.allanbank.tob.nov_13_2012.log;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * BatchTacticalLogWriter provides a implementation of writting a log to a
 * MongoDB collection where each log record is pre-aggregated around dimensions
 * of interest for each time period.
 * <p>
 * This implementation does the bulk of the aggregation in memory and pushes
 * accumulated updates at the end of the time period.
 * </p>
 * <p>
 * See the {@link TacticalLogWriter} JavaDoc for a discussion on the usage and
 * structure of the pre-aggregated documents.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchTacticalLogWriter {

    /** The collection to write to. */
    private final MongoCollection collection;

    /** The dimensions to use as the unique key for the aggregation. */
    private final AtomicReference<ConcurrentMap<Document, ConcurrentMap<String, Long>>> currentBatch;

    /** The dimensions to use as the unique key for the aggregation. */
    private volatile long currentTs;

    /** The dimensions to use as the unique key for the aggregation. */
    private final SortedSet<String> keyDimensions;

    /** The time period for the aggregation in milliseconds. */
    private final long timePeriodMs = TimeUnit.MINUTES.toMillis(5);

    /** The dimension to count unique values. */
    private final String valueDimension;

    /**
     * Creates a new SimpleLogWriter.
     * 
     * @param collection
     *            The collection to write to.
     * @param keyDimensions
     *            The dimensions to use as the unique key for the aggregation.
     */
    public BatchTacticalLogWriter(final MongoCollection collection,
            final Set<String> keyDimensions) {
        this(collection, keyDimensions, null);
    }

    /**
     * Creates a new SimpleLogWriter.
     * 
     * @param collection
     *            The collection to write to.
     * @param keyDimensions
     *            The dimensions to use as the unique key for the aggregation.
     * @param valueDimension
     *            The dimension to count unique values.
     */
    public BatchTacticalLogWriter(final MongoCollection collection,
            final Set<String> keyDimensions, final String valueDimension) {
        this.collection = collection;
        this.keyDimensions = Collections
                .unmodifiableSortedSet(new TreeSet<String>(keyDimensions));
        this.valueDimension = valueDimension;

        this.currentBatch = new AtomicReference<ConcurrentMap<Document, ConcurrentMap<String, Long>>>(
                new ConcurrentHashMap<Document, ConcurrentMap<String, Long>>());
        this.currentTs = currentTimePeriod();
    }

    /**
     * Writes the log record map as a record in the database. Each unique
     * {@code id} is stored as a single document with records pushed onto arrays
     * of various types.
     * 
     * @param record
     *            The record to write.
     */
    public void write(final Map<String, String> record) {

        // Note: doing the update as part of the event processing is easier but
        // means when not events are flowing no updates are sent and values may
        // be stranded in this object for an extended period of time.
        final long now = currentTimePeriod();
        if (now != currentTs) {
            commitBatch(now);
        }

        final DocumentBuilder id = BuilderFactory.start();
        final DocumentBuilder idDoc = id.push("_id");
        id.addTimestamp("ts", now);
        for (final String keyDimension : keyDimensions) {
            final String keyValue = record.get(keyDimension);
            if (keyValue != null) {
                idDoc.add(keyDimension, record.get(keyDimension));
            }
        }

        ConcurrentMap<String, Long> values = currentBatch.get().get(id.build());
        if (values == null) {
            final ConcurrentMap<String, Long> newValues = new ConcurrentHashMap<String, Long>();
            values = currentBatch.get().putIfAbsent(id.build(), newValues);
            if (values == null) {
                values = newValues;
            }
        }

        if (valueDimension == null) {
            increment(values, "count");
        }
        else {
            final String value = record.get(valueDimension);
            if (value != null) {
                increment(values, "value");
            }
            else {
                increment(values, "count");
            }
        }

        // No interaction with MonogDB.
    }

    /**
     * Determines the start of the current time period.
     * 
     * @return The start time of the current time period.
     */
    protected long currentTimePeriod() {
        final long now = System.currentTimeMillis();

        return (now - (now % timePeriodMs));
    }

    /**
     * Commits the current batch of updates if the timestamp is not equal to the
     * current timestamp.
     * 
     * @param now
     *            The timestamp for now.
     */
    private synchronized void commitBatch(final long now) {

        ConcurrentMap<Document, ConcurrentMap<String, Long>> batch = null;
        synchronized (this) {
            if (now != currentTs) {
                batch = currentBatch.get();
                currentBatch
                        .set(new ConcurrentHashMap<Document, ConcurrentMap<String, Long>>());
                currentTs = now;
            }
        }

        if (batch != null) {
            final List<Future<Long>> updateResults = new ArrayList<Future<Long>>(
                    batch.size());
            for (final Map.Entry<Document, ConcurrentMap<String, Long>> entry : batch
                    .entrySet()) {
                final Document id = entry.getKey();
                final DocumentBuilder update = BuilderFactory.start();
                final DocumentBuilder inc = update.push("$inc");
                for (final Map.Entry<String, Long> value : entry.getValue()
                        .entrySet()) {
                    inc.add(value.getKey(), value.getValue().longValue());
                }

                // Lot of information lost if the update fails. Lets make sure
                // it gets to the server and is handled.
                //
                // ... but wait! Still no reason to wait for each update.
                updateResults.add(collection.updateAsync(where("_id")
                        .equals(id), update, false /* multiupdate */,
                        true /* upsert */, Durability.ACK));
            }

            for (final Future<Long> updateResult : updateResults) {
                try {
                    updateResult.get();
                }
                catch (final InterruptedException e) {
                    // Handle...
                }
                catch (final ExecutionException e) {
                    // Handle... Likely the e.getCause() will be a
                    // MongoDbException.
                }
            }
        }
    }

    /**
     * Increments the value in the map.
     * 
     * @param values
     *            The values map.
     * @param name
     *            The name of the value it increment.
     */
    private void increment(final ConcurrentMap<String, Long> values,
            final String name) {
        while (true) {
            if (!values.containsKey(name)) {
                values.putIfAbsent(name, Long.valueOf(0));
            }
            else {
                final Long oldValue = values.get(name);
                final Long newValue = Long.valueOf(oldValue.longValue() + 1);
                if (values.replace(name, oldValue, newValue)) {
                    return;
                }
            }
        }
    }
}
