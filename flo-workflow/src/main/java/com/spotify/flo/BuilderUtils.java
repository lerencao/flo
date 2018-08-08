/*-
 * -\-\-
 * Flo Workflow Definition
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo;

import static com.spotify.flo.Values.toValueList;
import static java.util.stream.Collectors.toList;

import com.spotify.flo.EvalContext.Value;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Internal utility functions for the {@link TaskBuilder} api implementation
 */
class BuilderUtils {

  private BuilderUtils() {
  }

  /**
   * Converts an array of {@link Fn}s of {@link Task}s to a {@link Fn} of a list of
   * those tasks {@link Task}s.
   *
   * It will only evaluate the functions (through calling {@link Fn#get()})
   * when the returned function is invoked. Thus it retains laziness.
   *
   * @param tasks  An array of lazy evaluated tasks
   * @return A function of a list of lazily evaluated tasks
   */
  @SafeVarargs
  static Fn<List<Task<?>>> lazyList(Fn<? extends Task<?>>... tasks) {
    return () -> Stream.of(tasks)
        .map(Fn::get)
        .collect(toList());
  }

  @SafeVarargs
  static <T> Fn<List<T>> lazyFlatten(Fn<? extends List<? extends T>>... lists) {
    return () -> Stream.of(lists)
        .map(Fn::get)
        .flatMap(List::stream)
        .collect(toList());
  }

  static <T> List<T> appendToList(List<T> list, T t) {
    final List<T> newList = new ArrayList<>(list);
    newList.add(t);
    return newList;
  }

  static <T> ProcessFnArg contextArg(TaskContext<T> taskContext) {
    return ec -> ec.value(() -> taskContext.provide(ec));
  }

  static <T> ProcessFnArg inputArg(Fn<Task<T>> task) {
    return ec -> ec.evaluate(task.get());
  }

  static <T> ProcessFnArg inputsArg(Fn<List<Task<T>>> task) {
    return ec -> task.get().stream()
        .map(ec::evaluate)
        .collect(toValueList(ec));
  }
}