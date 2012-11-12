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

package com.allanbank.tob.nov_13_2012.esp;

import static com.allanbank.mongodb.builder.QueryBuilder.where;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.FindAndModify;

/**
 * SimpleEventHandler provides a simple version of the event handler for a event
 * stream processor.
 * 
 * @param <E>
 *            The input event type.
 * @param <O>
 *            The output event type.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SimpleEventHandler<E, O> {

    /** The business logic for evaluating the state of events. */
    protected final BusinessLogic<E, O> businessLogic;

    /** Factory for event ids and serialization of events. */
    protected final EventFactory<E> factory;

    /** Collection for storing events. */
    private final MongoCollection collection;

    /**
     * Creates a new AsyncEventHandler.
     * 
     * @param factory
     *            Factory for event ids and serialization of events.
     * @param businessLogic
     *            The business logic for evaluating the state of events.
     * @param collection
     *            Collection for storing events.
     */
    public SimpleEventHandler(final EventFactory<E> factory,
            final BusinessLogic<E, O> businessLogic,
            final MongoCollection collection) {
        this.factory = factory;
        this.businessLogic = businessLogic;
        this.collection = collection;
    }

    /**
     * Basic processing for a single event. Push a document onto the 'events'
     * list and then allow the business logic to determine what events should be
     * handled.
     * <p>
     * This is by no means a complete solution. Things still to be considered:
     * <ul>
     * <li>Concurrent updates to the state of a document. MongoDB does the right
     * thing but what about the business logic? Can we ensure only 1 result for
     * an event is generated.</li>
     * <li>Scheduling and handling timeouts for each document.</li>
     * <li>Handling the case of a process dying unexpectedly.</li>
     * <li>...</li>
     * <ul>
     * 
     * @param event
     *            The event to process.
     * @return The events created as a result of processing the {@code event}.
     * 
     * @see AsyncEventHandler#handleEvent(Object)
     */
    public List<O> handleEvent(final E event) {
        final byte[] bytes = factory.serialize(event);
        final ObjectId id = factory.getId(event);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$push").push("events").add("event", bytes)
                .add("ts", System.currentTimeMillis());
        try {
            final FindAndModify command = new FindAndModify.Builder()
                    .setQuery(where("_id").equals(id)).setUpdate(update)
                    .setReturnNew(true).setUpsert(true).build();

            final Document doc = collection.findAndModify(command);
            final List<E> events = deserialize(doc.get(ArrayElement.class,
                    "events"));
            final List<O> output = businessLogic.processEvents(events);

            return output;
        }
        catch (final RuntimeException re) {
            // Handle the error for the 'event'.
        }
        return Collections.emptyList();
    }

    /**
     * Deserialized the events in the {@link ArrayElement}.
     * 
     * @param arrayElement
     *            The array containing the events.
     * @return The deserialized events.
     */
    protected List<E> deserialize(final ArrayElement arrayElement) {
        final List<E> events = new ArrayList<E>(arrayElement.getEntries()
                .size());
        for (final Element element : arrayElement.getEntries()) {
            if (element instanceof DocumentElement) {
                final DocumentElement docEvent = (DocumentElement) element;

                events.add(factory.deserialize(docEvent.get(
                        BinaryElement.class, "event").getValue()));
            }
        }
        return events;
    }
}
