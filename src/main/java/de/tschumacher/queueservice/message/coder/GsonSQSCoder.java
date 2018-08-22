/*
 * Copyright 2015 Tobias Schumacher
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package de.tschumacher.queueservice.message.coder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSQSCoder<B> implements SQSCoder<B> {

  private final Gson gson;
  private final Class<B> clazz;

  public GsonSQSCoder(final Class<B> clazz) {
    this(new GsonBuilder().create(), clazz);
  }

  public GsonSQSCoder(Gson gson, final Class<B> clazz) {
    super();
    this.gson = gson;
    this.clazz = clazz;
  }

  @Override
  public B encode(final String content) {
    return this.gson.fromJson(content, this.clazz);
  }

  @Override
  public String decode(final B content) {
    return this.gson.toJson(content);
  }

}
