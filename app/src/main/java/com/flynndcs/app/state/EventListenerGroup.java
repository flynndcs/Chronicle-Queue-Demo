package com.flynndcs.app.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.flynndcs.app.dto.CommandDTO;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import net.openhft.chronicle.map.ChronicleMap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This thread handles the initial and continual handling of events from the event store and
 * populates application state. Each thread in this group listens to a shard (partition) of the
 * event stream, handling events of a subset of aggregates.
 */
public class EventListenerGroup {
  private final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
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

  public void start() throws SQLException {
    long i = 0L;
    for (ExecutorService service : executors) {
      service.submit(
          new EventShardListenerThread(executors.size(), i, metrics, READER, counts, dbHostname));
      i++;
    }
  }
}
