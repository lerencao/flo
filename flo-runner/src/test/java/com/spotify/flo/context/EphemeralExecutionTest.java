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

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.OperationExtractingContext;
import com.spotify.flo.OperationExtractingContext.Operation;
import com.spotify.flo.Task;
import com.spotify.flo.TaskBuilder.F1;
import com.spotify.flo.TaskContextStrict;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskOperator;
import com.spotify.flo.TaskOperator.Listener;
import com.spotify.flo.TaskOperator.Operation.Result;
import com.spotify.flo.freezer.EvaluatingContext;
import com.spotify.flo.freezer.PersistingContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A POC for ephemeral workflow execution.
 *
 * <p>The task graph and all operations are persisted to a database.
 *
 * <p>The workflow is executed by looping over all tasks and evaluating them when their inputs are ready. If a task
 * has a {@link TaskOperator}, the operation is started and persisted to the database.
 *
 * <p>Operations are polled for completion. When an operation is completed, the task is completed with the output of
 * the operation.
 *
 * <p>The workflow has completed when all tasks and operations have completed.
 *
 * <p>Each workflow execution iteration takes place in a new subprocess. No state is kept in memory.
 */
public class EphemeralExecutionTest {

  private static final boolean DEBUG = false;

  private static final String ROOT_TASK_ID = "root_task_id";
  private static final String TASK_PATHS = "task_paths";
  private static final String TASKS = "tasks";
  private static final String OPERATIONS = "operations";
  private static final String OPERATION_STATES = "operation_states";
  private static final String OPERATION_SCHEDULE = "operation_schedule";

  private static final Logger log = LoggerFactory.getLogger(EphemeralExecutionTest.class);

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    final String dbPath = temporaryFolder.newFolder().getAbsolutePath();
    final String persistedTasksDir = temporaryFolder.newFolder().getAbsolutePath();

    log.info("db path: {}", dbPath);

    // Persist task graph
    log.info("Persisting workflow to {}", persistedTasksDir);
    fork(() -> {
      persistWorkflow(dbPath, persistedTasksDir);
      return null;
    });

    for (int i = 0; ; i++) {
      log.info("Executing workflow (iteration={})", i);
      final Optional<Duration> sleep = fork(() -> runWorkflowTick(dbPath, persistedTasksDir));
      if (sleep.isPresent()) {
        log.info("Idle, waiting {}s", sleep.get().getSeconds());
        Thread.sleep(sleep.get().toMillis());
      } else {
        break;
      }
    }

    final TaskId rootTaskId = read(dbPath, ROOT_TASK_ID);
    final EvaluatingContext evaluatingContext = evaluationContext(persistedTasksDir);
    final String result = evaluatingContext.readExistingOutput(rootTaskId);

    log.info("workflow result: {}", result);

    assertThat(result, is("foo bar foo bar quux deadbeef"));
  }

  public static EvaluatingContext evaluationContext(String persistedTasksDir) {
    return new EvaluatingContext(Paths.get(persistedTasksDir), EvalContext.sync());
  }

  public static void persistWorkflow(String dbPath, String dir) {
    final Task<String> foo = Task.named("foo")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: foo");
          return "foo";
        });

    final Task<String> bar = Task.named("bar")
        .ofType(String.class)
        .process(() -> {
          log.info("process fn: bar");
          return "bar";
        });

    final Task<String> quux = Task.named("quux")
        .ofType(String.class)
        .context(new DoneOutput<>("quux"))
        .process(s -> {
          throw new AssertionError("execution of quux not expected");
        });

    final Task<String> deadbeef = Task.named("deadbeef")
        .ofType(String.class)
        .context(new DoneOutput<>("deadbeef"))
        .operator(Jobs.JobOperator.create())
        .process((s, j) -> {
          throw new AssertionError("execution of deadbeef not expected");
        });

    final Task<String> baz = Task.named("baz")
        .ofType(String.class)
        .operator(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .process((spec, a, b) -> {
          log.info("process fn: baz");
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r -> a + " " + b);
        });

    final Task<String> root = Task.named("root")
        .ofType(String.class)
        .operator(Jobs.JobOperator.create())
        .input(() -> foo)
        .input(() -> bar)
        .input(() -> baz)
        .input(() -> quux)
        .input(() -> deadbeef)
        .process((spec, a, b, c, d, e) -> {
          log.info("process fn: main");
          return spec
              .options(() -> ImmutableMap.of("foo", "bar"))
              .pipeline(ctx -> ctx
                  .readFrom("foo")
                  .map("_ + _")
                  .writeTo("bar"))
              .validation(r -> {
                if (r.records == 0) {
                  throw new AssertionError("no records seen!");
                }
              })
              .success(r -> String.join(" ", a, b, c, d, e));
        });

    final PersistingContext persistingContext = new PersistingContext(
        Paths.get(dir), EvalContext.sync());

    try {
      MemoizingContext.composeWith(persistingContext)
          .evaluate(root).toFuture().exceptionally(t -> "").get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final Map<TaskId, String> tasks = new HashMap<>();
    tasks(root, t -> tasks.put(t.id(), persistingContext.getFiles().get(t.id()).toAbsolutePath().toString()));

    withRocksDB(dbPath, db -> {
      write(db, TASKS, new HashSet<>(tasks.keySet()));
      write(db, TASK_PATHS, tasks);
      write(db, ROOT_TASK_ID, root.id());
      return null;
    });
  }

  private static Optional<Duration> runWorkflowTick(String dbPath, String persistedTasksDir) {
    return withRocksDB(dbPath, db -> {
      try {
        return runWorkflowTick0(db, persistedTasksDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static Optional<Duration> runWorkflowTick0(RocksDB db, String persistedTasksDir) throws RocksDBException, IOException {
    final Set<TaskId> tasks = read(db, TASKS, HashSet::new);
    final Map<TaskId, String> taskPaths = read(db, TASK_PATHS, HashMap::new);
    final Map<TaskId, TaskOperator.Operation> operations = read(db, OPERATIONS, HashMap::new);
    final Map<TaskId, Object> operationStates = read(db, OPERATION_STATES, HashMap::new);
    final Map<TaskId, Instant> operationSchedule = read(db, OPERATION_SCHEDULE, HashMap::new);

    log.info("#tasks={}, #operations={}", tasks.size(), operations.size());

    // Evaluate tasks
    final Iterator<TaskId> taskIt = tasks.iterator();
    boolean progressed = false;
    while (taskIt.hasNext()) {
      final ByteArrayInputStream bais = new ByteArrayInputStream(
          Files.readAllBytes(Paths.get(taskPaths.get(taskIt.next()))));
      final Task<?> task = PersistingContext.deserialize(bais);
      final boolean inputReady = task.inputs().stream().map(Task::id)
          .allMatch(evaluationContext(persistedTasksDir)::isEvaluated);
      if (!inputReady) {
        log.info("task {}: inputs not yet ready", task.id());
        continue;
      }

      progressed = true;

      final String taskPath = taskPaths.get(task.id()).toString();

      // Check if output has already been produced and execution can be skipped
      @SuppressWarnings("unchecked") final Optional<TaskContextStrict<?, Object>> outputContext =
          (Optional) TaskContextStrict.taskContextStrict(task);
      if (outputContext.isPresent()) {
        @SuppressWarnings("unchecked") final Optional<Object> lookup = outputContext.get()
            .lookup((Task<Object>) task);
        if (lookup.isPresent()) {
          log.info("task {}: output already exists, not executing", task.id());
          evaluationContext(persistedTasksDir).persist(task.id(), lookup.get());
          taskIt.remove();
          continue;
        }
      }

      // Execute task
      if (OperationExtractingContext.operator(task).isPresent()) {
        // Start operation
        log.info("task {}: extracting operation", task.id());
        final Operation op = fork(() ->
            OperationExtractingContext.extract(task, taskOutput(evaluationContext(persistedTasksDir))));
        final TaskOperator.Operation operation = op.operator.start(op.spec, Listener.NOP);
        log.info("task {}: operation {}: starting", task.id(), operation);
        operations.put(task.id(), operation);
      } else {
        // Execute generic task
        log.info("task {}: executing generic task", task.id());
        fork(() -> {
          try {
            return evaluationContext(persistedTasksDir).evaluateTaskFrom(Paths.get(taskPath)).toFuture().get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
        log.info("task {}: generic task completed", task.id());
      }
      taskIt.remove();
    }

    // Run operators
    final Iterator<Entry<TaskId, TaskOperator.Operation>> opIt = operations.entrySet().iterator();
    while (opIt.hasNext()) {
      final Entry<TaskId, TaskOperator.Operation> e = opIt.next();
      final TaskId taskId = e.getKey();

      // Due for polling?
      final Instant pollDeadline = operationSchedule.getOrDefault(taskId, Instant.MIN);
      if (Instant.now().isBefore(pollDeadline)) {
        continue;
      }

      // Poll!
      final TaskOperator.Operation operation = e.getValue();
      log.info("task {}: operation {}: polling", taskId, operation);
      Optional<Object> state = Optional.ofNullable(operationStates.get(taskId));
      final Result result = operation.perform(state, Listener.NOP);

      // Done?
      if (!result.isDone()) {
        log.info("task {}: operation {}: not done, checking again in {}", taskId, operation, result.pollInterval());
        operationSchedule.put(taskId, Instant.now().plus(result.pollInterval()));
        result.state().ifPresent(s -> operationStates.put(taskId, s));
        continue;
      }

      // Done!
      log.info("task {}: operation {}: completed", taskId, operation);
      progressed = true;
      operationStates.remove(taskId);
      operationSchedule.remove(taskId);

      if (result.isSuccess()) {
        evaluationContext(persistedTasksDir).persist(taskId, result.output());
      } else {
        throw new RuntimeException(result.cause());
      }

      opIt.remove();
    }

    // Sanity check
    assertThat(operations.keySet(), is(operationSchedule.keySet()));
    for (TaskId taskId : operationSchedule.keySet()) {
      assertThat(operations, hasKey(taskId));
    }

    // Any tasks or operations left?
    if (tasks.isEmpty() && operations.isEmpty()) {
      log.info("all tasks and operations completed");
      return Optional.empty();
    }

    write(db, TASKS, tasks);
    write(db, OPERATIONS, operations);
    write(db, OPERATION_STATES, operationStates);
    write(db, OPERATION_SCHEDULE, operationSchedule);

    if (progressed) {
      return Optional.of(Duration.ofSeconds(0));
    }

    final Instant nextPoll = operationSchedule.values().stream()
        .min(Comparator.naturalOrder())
        .orElseThrow(AssertionError::new);
    return Optional.of(Duration.between(Instant.now(), nextPoll));
  }

  private static <T> T fork(Fn<T> f) {
    if (DEBUG) {
      return f.get();
    }
    try (ForkingExecutor executor = new ForkingExecutor()) {
      return executor.execute(f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void write(String dbPath, String key, Object value) {
    withRocksDB(dbPath, db -> write(db, key, value));
  }

  private static Object write(RocksDB db, String key, Object value) throws RocksDBException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PersistingContext.serialize(value, baos);
    db.put(key.getBytes(StandardCharsets.UTF_8), baos.toByteArray());
    return null;
  }

  private static <T> T read(String dbPath, String key) {
    return withRocksDB(dbPath, db -> read(db, key, () -> null));
  }

  private static <T> T read(String dbPath, String key, Fn<T> d) {
    return withRocksDB(dbPath, db -> read(db, key, d));
  }

  private static <T> T read(RocksDB db, String key, Fn<T> defaultValue) throws RocksDBException {
    final byte[] bytes = db.get(key.getBytes(StandardCharsets.UTF_8));
    if (bytes == null) {
      return defaultValue.get();
    }
    final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    return PersistingContext.deserialize(bais);
  }

  private static <T> T withRocksDB(final String dbPath, RocksDBFn<T> f) {
    RocksDB.loadLibrary();
    try (final Options options = new Options().setCreateIfMissing(true)) {
      try (final RocksDB db = RocksDB.open(options, dbPath)) {
        try {
          return f.get(db);
        } catch (RocksDBException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private static F1<TaskId, ?> taskOutput(EvaluatingContext evaluatingContext) {
    return taskId -> {
      try {
        return evaluatingContext.readExistingOutput(taskId);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  private static void tasks(Task<?> task, Consumer<Task<?>> f) {
    f.accept(task);
    task.inputs().forEach(upstream -> tasks(upstream, f));
  }

  private static class DoneOutput<T> extends TaskContextStrict<String, T> {

    private final T value;

    public DoneOutput(T value) {
      this.value = value;
    }

    @Override
    public String provide(EvalContext evalContext) {
      return "";
    }

    @Override
    public Optional<T> lookup(Task<T> task) {
      return Optional.of(value);
    }
  }

  private interface RocksDBFn<T> {

    T get(RocksDB db) throws RocksDBException;
  }
}
