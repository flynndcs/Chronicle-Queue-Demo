package com.flynndcs.app.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.flynndcs.app.dto.CommandDTO;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import net.openhft.chronicle.map.ChronicleMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

public class EventShardListenerThread implements Runnable {
  private final ThreadLocal<Long> latestSequenceId;
  private final ThreadLocal<PreparedStatement> readStatement;
  private final ThreadLocal<Timer> timer;
  private final ThreadLocal<CommandDTO> COMMAND_DTO_EVENT;
  private static ChronicleMap<Long, Long> counts;
  private final ThreadLocal<ObjectReader> READER;
  private final ThreadLocal<Connection> connection;
  private final ThreadLocal<ResultSet> resultSet;
  private final ThreadLocal<ChronicleMapProjector> projector;
  private final ThreadLocal<Long> index;
  private static Long numThreads;

  public EventShardListenerThread(
      long poolsize,
      long threadIndex,
      PrometheusMeterRegistry metricsRegistry,
      ChronicleMap<Long, Long> countsMap,
      String dbHost) {
    numThreads = poolsize;
    index = ThreadLocal.withInitial(() -> threadIndex);
    latestSequenceId = ThreadLocal.withInitial(() -> -1L);
    timer =
        ThreadLocal.withInitial(
            () ->
                Timer.builder("updateState" + threadIndex)
                    .publishPercentiles(0.5, 0.9, 0.99)
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofNanos(1000))
                    .maximumExpectedValue(Duration.ofSeconds(2))
                    .register(metricsRegistry));
    COMMAND_DTO_EVENT = ThreadLocal.withInitial(CommandDTO::new);
    READER = ThreadLocal.withInitial(() -> new ObjectMapper().readerFor(CommandDTO.class));
    counts = countsMap;
    // each thread gets its own connection to event store
    connection =
        ThreadLocal.withInitial(
            () -> {
              try {
                return (DriverManager.getConnection(
                    "jdbc:postgresql://" + dbHost + ":5432/postgres", "postgres", "postgres"));
              } catch (SQLException e) {
                e.printStackTrace();
                return null;
              }
            });
    // only retrieve the events that partition into our bucket and are more recent than what we've
    // processed previously
    readStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection
                    .get()
                    .prepareStatement(
                        "SELECT * FROM events WHERE event_sequence_id > ? AND MOD(aggregate_id, "
                            + numThreads
                            + ") = "
                            + threadIndex
                            + " ORDER BY event_sequence_id ASC");
              } catch (SQLException e) {
                e.printStackTrace();
                return null;
              }
            });
    resultSet = ThreadLocal.withInitial(() -> null);
    projector = ThreadLocal.withInitial(ChronicleMapProjector::new);
  }

  @Override
  public void run() {
    try {
      while (true) {
        try {
          // latestSequenceId is -1 or the value of the last event this thread saw
          System.out.println("previous event: " + latestSequenceId.get());
          // prepare statement
          readStatement.get().setLong(1, latestSequenceId.get());
          // execute query and store results
          resultSet.set(readStatement.get().executeQuery());
          System.out.println(
              "listening for events from event store after event id "
                  + latestSequenceId.get()
                  + " and aggregate_id % poolsize = "
                  + index.get());

          // for each record
          while (resultSet.get().next()) {
            timer
                .get()
                .record(
                    () -> {
                      try {
                        // deserialize into the COMMAND_DTO_EVENT object
                        COMMAND_DTO_EVENT.set(
                            READER
                                .get()
                                .readValue(
                                    resultSet.get().getString(resultSet.get().findColumn("data"))));
                        // apply the event content to our application state (projection)
                        synchronized (this) {
                          projector.get().projectToCountsMap(COMMAND_DTO_EVENT.get(), counts);
                        }
                        // use the event's sequence id as our new latest event processed
                        latestSequenceId.set(
                            resultSet
                                .get()
                                .getLong(resultSet.get().findColumn("event_sequence_id")));
                      } catch (Exception ex) {
                        ex.printStackTrace();
                      }
                    });
          }
          Thread.sleep(1000);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
