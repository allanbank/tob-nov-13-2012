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

import java.util.Map;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * SimpleAggregateLogWriter provides a simple implementation of writting a log
 * to a MongoDB collection where each record about a specific entity is
 * aggregated into a single document. A resulting document looks like:
 * <blockquote>
 * 
 * <pre>
 * <code>
 * {
 *    _id : "0000-0000-0000-0001",
 *    trans : [ { store : “Amazon”, ts : UTC(2012-11-13T01:00:00Z), accepted : true } ],
 *    lost: [ { ts : UTC(2012-11-12T01:00:00Z) ],
 * }
 * </code>
 * </pre>
 * 
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleAggregateLogWriter {

    /** The collection to write to. */
    private final MongoCollection collection;

    /**
     * Creates a new SimpleLogWriter.
     * 
     * @param collection
     *            The collection to write to.
     */
    public SimpleAggregateLogWriter(final MongoCollection collection) {
        this.collection = collection;
    }

    /**
     * Writes the log record map as a record in the database. Each unique
     * {@code id} is stored as a single document with records pushed onto arrays
     * based on the various type of the record.
     * 
     * @param id
     *            The domain id the log record relaates to.
     * @param type
     *            The "type" of the event. Used for course aggregation.
     * @param record
     *            The record to write.
     */
    public void write(final String id, final String type,
            final Map<String, String> record) {

        final DocumentBuilder document = BuilderFactory.start();
        document.addTimestamp("ts", System.currentTimeMillis());
        for (final Map.Entry<String, String> element : record.entrySet()) {
            document.add(element.getKey(), element.getValue());
        }

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$push").add(type, document);

        // Fire and forget!
        collection.update(where("_id").equals(id), update,
                false /* multiupdate */, true /* upsert */, Durability.NONE);
    }
}
