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

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/**
 * An operator controls the execution of a job for a task,  e.g. a data processing job on some processing platform.
 *
 * <p>The concrete operator implementation should {@link #provide(EvalContext)} the task with some means of constructing
 * an operation description. The operation description should be returned from the process fn.
 */
public interface TaskOperator<ContextT, SpecT, ResultT>
    extends TaskContext<ContextT> {

  @SuppressWarnings("unchecked")
  default ResultT perform(SpecT spec, Listener listener) {
    final Operation<ResultT, ?> operation = start(spec, listener);
    Optional state = Optional.empty();
    Operation.Result<ResultT, ?> result = operation.perform(state, listener);
    while (!result.isDone()) {
      try {
        Thread.sleep(result.pollInterval().toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    if (result.isSuccess()) {
      return result.output();
    } else {
      final Throwable cause = result.cause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  Operation<ResultT, ?> start(SpecT spec, Listener listener);

  interface Listener extends Serializable {

    /**
     * Called to report some piece of task metadata.
     *
     * @param task The task that is being evaluated
     * @param key The metadata key.
     * @param value The metadata value.
     */
    void meta(TaskId task, String key, String value);

    Listener NOP = (Listener) (task, key, value) -> {
    };

    default Listener composeWith(Listener listener) {
      return (task, key, value) -> {
        meta(task, key, value);
        listener.meta(task, key, value);
      };
    }
  }

  interface Operation<ResultT, StateT> extends Serializable {

    Result<ResultT, StateT> perform(Optional<StateT> state, Listener listener);

    interface Result<ResultT, StateT> extends Serializable {

      boolean isDone();

      ResultT output();

      Optional<StateT> state();

      Duration pollInterval();

      boolean isSuccess();

      Throwable cause();

      static <ResultT> Result<ResultT, Void> ofContinuation(Duration pollInterval) {
        return ofContinuation(pollInterval, null);
      }

      static <ResultT, StateT> Result<ResultT, StateT> ofContinuation(Duration pollInterval, StateT state) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return false;
          }

          @Override
          public ResultT output() {
            throw new IllegalStateException("not done");
          }

          @Override
          public Optional<StateT> state() {
            return Optional.ofNullable(state);
          }

          @Override
          public Duration pollInterval() {
            return pollInterval;
          }

          @Override
          public boolean isSuccess() {
            throw new IllegalStateException("not done");
          }

          @Override
          public Throwable cause() {
            throw new IllegalStateException("not done");
          }
        };
      }

      static <ResultT, StateT> Result<ResultT, StateT> ofSuccess(ResultT output) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public ResultT output() {
            return output;
          }

          @Override
          public Optional<StateT> state() {
            return Optional.empty();
          }

          @Override
          public Duration pollInterval() {
            throw new IllegalStateException("operation is done");
          }

          @Override
          public boolean isSuccess() {
            return true;
          }

          @Override
          public Throwable cause() {
            throw new IllegalStateException("operation is a success");
          }
        };
      }

      static <ResultT, StateT> Result<ResultT, StateT> ofFailure(Throwable cause) {
        return new Result<ResultT, StateT>() {
          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public ResultT output() {
            throw new IllegalStateException("operation is a failure");
          }

          @Override
          public Optional<StateT> state() {
            return Optional.empty();
          }

          @Override
          public Duration pollInterval() {
            throw new IllegalStateException("operation is done");
          }

          @Override
          public boolean isSuccess() {
            return false;
          }

          @Override
          public Throwable cause() {
            return cause;
          }
        };
      }
    }
  }
}
