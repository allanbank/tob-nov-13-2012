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

import com.allanbank.mongodb.bson.element.ObjectId;

/**
 * EventFactory provides an interface for returning an id of an event and to
 * serialize and deserialize the events.
 * 
 * @param <E>
 *            The type of event handled by this factory.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface EventFactory<E> {
    /**
     * Deserializes an event stored in MongoDB.
     * 
     * @param event
     *            The serialized form of the event.
     * @return The deserialized event.
     */
    E deserialize(byte[] event);

    /**
     * Returns a unique id for the event's correlation. All events that should
     * be analyzed together should return the same id.
     * 
     * @param event
     *            The event to be correlated.
     * @return The id for the event correlation.
     */
    ObjectId getId(E event);

    /**
     * Serializes an event for storage in MongoDB.
     * 
     * @param event
     *            The event to serialize.
     * @return The serialized form of the event.
     */
    byte[] serialize(E event);
}
