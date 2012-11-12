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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Aggregate;

/**
 * BatchTacticalLogWriter provides a implementation of writting a log to a
 * MongoDB collection where each log record is pre-aggregated around dimensions
 * of interest for each time period.
 * <p>
 * Input to this class are records that may look like (using Google Analytics as
 * an example): <blockquote>
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
 * </blockquote>
 * <p>
 * The purpose of this class is to make near real time analytics of the data
 * available by focusing on specific pre-determined dimensions of interest. As
 * an example suppose we want to track how many page views there are across the
 * dimensions ( location_country, browser, os, screen_resolution ) we can create
 * a {@link BatchTacticalLogWriter} like this:<blockquote>
 * 
 * <pre>
 * <code>
 *  MongoCollection collection = ...;
 *  Set<String> dimensions = new HashSet<String>();
 *  dimensions.add("location_country");
 *  dimensions.add("browser");
 *  dimensions.add("os");
 *  dimensions.add("screen_resolution");
 *  BatchTacticalLogWriter writer = new {@link BatchTacticalLogWriter}(collection, 
 *              dimensions );
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * This log writer will then create documents in MongoDB like the following.
 * Note that the timestamp for all records within a 5 minute interval will be
 * adjusted to the beginning of the time period. So a log record received on
 * 2012-11-13 at 05:37:23 may result in the document: <blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     _id {
 *         ts: UTC( '2012-11-13T05:35:00' ),
 *         browser: 'Chrome',
 *         location_country : 'US',
 *         os : 'Linux',
 *         screen_resolution : '1920x1080',
 *     },
 *     count : 1
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * We can also store one of the dimensions as the counts within the result
 * document. To extend the example we could use the counts to track the search
 * provider: <blockquote>
 * 
 * <pre>
 * <code>
 *  MongoCollection collection = ...;
 *  Set<String> dimensions = new HashSet<String>();
 *  dimensions.add("location_country");
 *  dimensions.add("browser");
 *  dimensions.add("os");
 *  dimensions.add("screen_resolution");
 *  BatchTacticalLogWriter writer = new {@link BatchTacticalLogWriter}(collection, 
 *              dimensions, "search_provider" );
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * This log writer will then create documents in MongoDB like:<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     _id {
 *         ts: UTC( '2012-11-13T05:35:00' ),
 *         browser: 'Chrome',
 *         location_country : 'US',
 *         os : 'Linux',
 *         screen_resolution : '1920x1080',
 *     },
 *     google : 10,
 *     bing : 1
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * For maximum performance we will want to create a hash of the _id and use that
 * instead of the document. We will want to maintain the current {@code _id}
 * document in the results so they can be used in queries. Something
 * like:<blockquote>
 * 
 * <pre>
 * <code>
 * {
 *     _id : '0123456789ABCDEF', // Hash.
 *     id {
 *         ts: UTC( '2012-11-13T05:35:00' ),
 *         browser: 'Chrome',
 *         location_country : 'US',
 *         os : 'Linux',
 *         screen_resolution : '1920x1080',
 *     },
 *     google : 10,
 *     bing : 1
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * </p>
 * <p>
 * Creating the hashed id is left as an exercide for the reader.
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
 *         .match(where("id.ts").greaterThanOrEqualToTimestamp(
 *                                     sdf.parse("2012-11-13T05:00:00").getTime())
 *                              .lessThanTimestamp(
 *                                     sdf.parse("2012-11-13T06:00:00").getTime())
 *                 .and("id.os").equals("Windows")
 *                 .and("id.browser").equals("Chrome")
 *                 .and("id.screen_resolution").equals("1920x1080")
 *                 .and("id.location_country").equals("AU"))
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
 * queries it is advisable to create an index on the {@code id.ts} field.
 * </p>
 * 
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TacticalLogWriter {

    /** The collection to write to. */
    private final MongoCollection collection;

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
     * @param valueDimension
     *            The dimension to count unique values.
     */
    public TacticalLogWriter(final MongoCollection collection,
            final Set<String> keyDimensions, final String valueDimension) {
        this.collection = collection;
        this.keyDimensions = Collections
                .unmodifiableSortedSet(new TreeSet<String>(keyDimensions));
        this.valueDimension = valueDimension;
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

        final DocumentBuilder id = BuilderFactory.start();
        final DocumentBuilder idDoc = id.push("_id");
        id.addTimestamp("ts", currentTimePeriod());
        for (final String keyDimension : keyDimensions) {
            final String keyValue = record.get(keyDimension);
            if (keyValue != null) {
                idDoc.add(keyDimension, record.get(keyDimension));
            }
        }

        final DocumentBuilder update = BuilderFactory.start();
        if (valueDimension == null) {
            update.push("$inc").add("count", 1);
        }
        else {
            final String value = record.get(valueDimension);
            if (value != null) {
                update.push("$inc").add(value, 1);
            }
            else {
                update.push("$inc").add("count", 1);
            }
        }

        // Fire and forget!
        collection.update(where("_id").equals(id), update,
                false /* multiupdate */, true /* upsert */, Durability.NONE);
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
                .match(where("id.ts")
                        .greaterThanOrEqualToTimestamp(
                                sdf.parse("2012-11-13T05:00:00").getTime())
                        .lessThanTimestamp(
                                sdf.parse("2012-11-13T06:00:00").getTime())
                        .and("id.os").equals("Windows").and("id.browser")
                        .equals("Chrome").and("id.screen_resolution")
                        .equals("1920x1080").and("id.location_country")
                        .equals("AU"))
                .group(constantId("count"), set("sum").sum("count")).build();

        final List<Document> results = collection.aggregate(command);
        System.out.println(results);
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
}
