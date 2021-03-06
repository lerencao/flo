/*-
 * -\-\-
 * Flo Runner
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.flo.context;

import com.spotify.flo.EvalContext;
import com.spotify.flo.TaskBuilder.F0;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

class Jobs {

  static class JobSpec<T> implements Serializable {

    private final TaskId taskId;

    private F0<Map<String, ?>> options = Collections::emptyMap;
    private SerializableConsumer<JobContext> pipelineConfigurator = ctx -> {};
    private SerializableConsumer<JobResult> resultValidator = result -> {};
    private F1<JobResult, T> successHandler;

    JobSpec(TaskId taskId) {
      this.taskId = taskId;
    }

    public JobSpec<T> options(F0<Map<String, ?>> options) {
      this.options = options;
      return this;
    }

    public JobSpec<T> pipeline(SerializableConsumer<JobContext> pipeline) {
      this.pipelineConfigurator = pipeline;
      return this;
    }

    public JobSpec<T> validation(SerializableConsumer<JobResult> validator) {
      this.resultValidator = validator;
      return this;
    }

    public JobSpec<T> success(F1<JobResult, T> successHandler) {
      this.successHandler = successHandler;
      return this;
    }
  }

  static class JobOperator<T> implements TaskOperator<JobSpec<T>, JobSpec<T>, T> {

    @Override
    public JobSpec<T> provide(EvalContext evalContext) {
      return new JobSpec<>(evalContext.currentTask().get().id());
    }

    static <T> JobOperator<T> create() {
      return new JobOperator<>();
    }

    @Override
    public T perform(JobSpec<T> spec, Listener listener) {
      final JobContext jobContext = new JobContext(spec.options.get());
      listener.meta(spec.taskId, Collections.singletonMap("task-id", spec.taskId.toString()));
      spec.pipelineConfigurator.accept(jobContext);
      final JobResult result = jobContext.run();
      spec.resultValidator.accept(result);
      return spec.successHandler.apply(result);
    }
  }

  static class JobContext {

    public JobContext(Map<String, ?> options) {
    }

    public JobContext readFrom(String src) {
      return this;
    }

    public JobContext map(String operation) {
      return this;
    }


    public JobContext writeTo(String dst) {
      return this;
    }

    public JobResult run() {
      return new JobResult(4711);
    }
  }

  static class JobResult {

    final int records;

    public JobResult(int records) {
      this.records = records;
    }
  }
}
