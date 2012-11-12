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

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.constantId;
import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * SimpleLogWriter provides a simple implementation of writting a log to a
 * MongoDB collection.
 * <p>
 * This writer simply puts each document into the collection with a new
 * {@link ObjectId} and with the addition of a timestamp. Input to this class
 * are records that may look like (using Google Analytics as an
 * example):<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     id: 'UA-33634837',
 *     url: '/mongodb-async-driver/',
 *     lang: 'en',
 *     location_country : 'US',
 *     location_city : 'Baltimore',
 *     browser: 'Chrome',
 *     os : 'Linux',
 *     network_provider : 'Verizon',
 *     search_provider : 'google',
 *     search_term : 'mongodb fastest java driver',
 *     user_id : '123456',
 *     ...
 * }
 * </code>
 * </pre>
 * 
 * </blockquote> The class will then create documents that look like
 * <blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     _id : ObjectId(),
 *     ts: UTC( '2012-11-13T05:35:00' ),
 *     id: 'UA-33634837',
 *     url: '/mongodb-async-driver/',
 *     lang: 'en',
 *     location_country : 'US',
 *     location_city : 'Baltimore',
 *     browser: 'Chrome',
 *     os : 'Linux',
 *     network_provider : 'Verizon',
 *     search_provider : 'google',
 *     search_term : 'mongodb fastest java driver',
 *     user_id : '123456',
 *     ...
 * }
 * </code>
 * </pre>
 * 
 * </blockquote> This has a limited scalability since the ObjectId {@code _id}
 * field will case all of the writes to be sent to a single MongoDB
 * server/replica-set. The solution is to instead use a hash of all of the
 * values in the document. This is left as an exercide of the reader.
 * </p>
 * <h3>Runtime Aggregation</h3>
 * <p>
 * With the preaggregated documents we can perform realtime analytics using the
 * MongoDB aggregation framework. Support we want the total number of page views
 * between 05:00 and 06:00 on 2012-11-13 for the Windows OS using Chrome browser
 * with a resolution of 1920x1080 in Australia?<blockquote>
 * 
 * <pre>
 * <code>
 * SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
 * Aggregate command = new Aggregate.Builder()
 *         .match(where("ts").greaterThanOrEqualToTimestamp(
 *                                     sdf.parse("2012-11-13T05:00:00").getTime())
 *                               .lessThanTimestamp(
 *                                     sdf.parse("2012-11-13T06:00:00").getTime())
 *                 .and("os").equals("Windows")
 *                 .and("browser").equals("Chrome")
 *                 .and("screen_resolution").equals("1920x1080")
 *                 .and("location_country").equals("AU"))
 *         .group(constantId("count"), set("sum").sum("count")).build();
 * 
 * List<Document> results = collection.aggregate(command);
 * </code>
 * </pre>
 * 
 * </blockquote>
 * <p>
 * The results should be a single document that looks like:<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     _id: 'count',
 *     sum: 213
 * }
 * </pre>
 * 
 * </code> </blockquote> Since most queries will likely use the timestamps for
 * queries it is advisable to create an index on the {@code ts} field.
 * </p>
 * 
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleLogWriter {

    /** The collection to write to. */
    private final MongoCollection collection;

    /**
     * Creates a new SimpleLogWriter.
     * 
     * @param collection
     *            The collection to write to.
     */
    public SimpleLogWriter(final MongoCollection collection) {
        this.collection = collection;
    }

    /**
     * Writes the log record map as a record in the database.
     * 
     * @param record
     *            The record to write.
     */
    public void write(final Map<String, String> record) {
        final DocumentBuilder document = BuilderFactory.start();
        document.addTimestamp("ts", System.currentTimeMillis());
        for (final Map.Entry<String, String> element : record.entrySet()) {
            document.add(element.getKey(), element.getValue());
        }

        // Fire and forget!
        collection.insert(Durability.NONE, document);
    }

    /**
     * An example aggregate command over the generated documents.
     * 
     * @throws ParseException
     *             On a failure parsing the test dates.
     */
    protected void aggregate() throws ParseException {
        // Between 05:00 and 06:00 on 2012-11-13 for the Windows OS using Chrome
        // browser with a resolution of 1920x1080 from Austrailia?
        final SimpleDateFormat sdf = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss");
        final Aggregate command = new Aggregate.Builder()
                .setReadPreference(ReadPreference.PREFER_SECONDARY)
                .match(where("ts")
                        .greaterThanOrEqualToTimestamp(
                                sdf.parse("2012-11-13T05:00:00").getTime())
                        .lessThanTimestamp(
                                sdf.parse("2012-11-13T06:00:00").getTime())
                        .and("os").equals("Windows").and("browser")
                        .equals("Chrome").and("screen_resolution")
                        .equals("1920x1080").and("location_country")
                        .equals("AU"))
                .group(constantId("count"), set("pageviews").count()).build();

        final List<Document> results = collection.aggregate(command);
        System.out.println(results);
    }
}
