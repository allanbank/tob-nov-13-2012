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

import java.util.List;

/**
 * BusinessLogic provides an interface for encapsulating business logic within
 * the event stream processing.
 * <p>
 * <blockquote>
 * 
 * <pre>
 * <code>
 * List<I> input = ...;
 * BusinessLogic<I,O> logic = ...;
 * 
 * List<O> results = logic.processEvents(input);
 * if( !logic.isDone() ) {
 *    long timeout = logic.nextTimeout();
 *    
 *    scheduleTimeout(timeout);
 * } else {
 *    remove();
 * }
 * </code>
 * </pre>
 * 
 * </blockquote>
 * 
 * @param <INPUT_EVENT>
 *            The input event type for the business logic.
 * @param <OUTPUT_EVENT>
 *            The output event type of the business logic.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface BusinessLogic<INPUT_EVENT, OUTPUT_EVENT> {
    /**
     * If true the last set of events processed can be deleted from the
     * persistent store.
     * 
     * @return True the last set of events processed can be deleted from the
     *         persistent store.
     */
    public boolean isDone();

    /**
     * The time that the event state should be re-evaluated.
     * 
     * @return The time that the event state should be re-evaluated.
     */
    public long nextTimeOut();

    /**
     * Process the input events and return 0-N output events as a result of the
     * analysis.
     * 
     * @param input
     *            The complete set of input events.
     * @return The set of output events from the analysis of the input events.
     */
    public List<OUTPUT_EVENT> processEvents(List<INPUT_EVENT> input);
}
