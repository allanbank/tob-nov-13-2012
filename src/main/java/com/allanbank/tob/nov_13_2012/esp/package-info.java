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

/**
 * Sample code for performing event stream processing using MongoDB.
 * <p>
 * Not a full solution but demonstrates the process of moving from the 
 * {@link com.allanbank.tob.nov_13_2012.esp.SimpleEventHandler#handleEvent synchronous} 
 * interface to the 
 * {@link com.allanbank.tob.nov_13_2012.esp.AsyncEventHandler#handleEvent asynchronous} 
 * interface.
 * <p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
package com.allanbank.tob.nov_13_2012.esp;