package com.flynndcs.app.state;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import net.openhft.chronicle.map.ChronicleMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This thread handles the initial and continual handling of events from the event store and
 * populates application state. Each thread in this group listens to a shard (partition) of the
 * event stream, handling events of a subset of aggregates.
 */
public class EventListenerGroup {
  private static PrometheusMeterRegistry metrics;
  private final List<ExecutorService> executors = new ArrayList<>();

  private final String dbHostname;
  private final ChronicleMap<Long, Long> counts;

  public EventListenerGroup(
      String dbHost,
      ChronicleMap<Long, Long> countsMap,
      int poolSize,
      PrometheusMeterRegistry metricsRegistry) {
    for (int i = 0; i < poolSize; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
    dbHostname = dbHost;
    counts = countsMap;
    metrics = metricsRegistry;
  }

  public void start() {
    for (int j = 0, executorsSize = executors.size(); j < executorsSize; j++) {
      ExecutorService service = executors.get(j);
      // start each listener thread with knowledge of the num of threads, its index, metrics, the application state
      service.submit(
          new EventShardListenerThread(executors.size(), j, metrics, counts, dbHostname));
    }
  }
}
