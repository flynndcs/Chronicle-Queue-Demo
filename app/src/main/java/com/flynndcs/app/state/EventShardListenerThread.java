package com.flynndcs.app.state;

import com.fasterxml.jackson.core.JsonProcessingException;
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
  private ThreadLocal<Long> index;
  private static long poolSize;
  private ThreadLocal<Long> latestSequenceId = ThreadLocal.withInitial(() -> 0L);
  private final ThreadLocal<PreparedStatement> readStatement;
  private static ThreadLocal<Timer> timer;
  private ThreadLocal<CommandDTO> COMMAND_DTO_EVENT = ThreadLocal.withInitial(CommandDTO::new);
  private static ChronicleMap<Long, Long> counts;

  private final ObjectReader READER;

  public EventShardListenerThread(
      long numThreads,
      long threadIndex,
      PrometheusMeterRegistry metricsRegistry,
      ObjectReader reader,
      ChronicleMap<Long, Long> countsMap,
      String dbHost) throws SQLException {
    poolSize = numThreads;
    index = ThreadLocal.withInitial(() -> threadIndex);
    timer =
        ThreadLocal.withInitial(
            () ->
                Timer.builder("updateState" + index.get().toString())
                    .publishPercentiles(0.5, 0.9, 0.99)
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofNanos(1000))
                    .maximumExpectedValue(Duration.ofSeconds(2))
                    .register(metricsRegistry));
    READER = reader;
    counts = countsMap;
    Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://" + dbHost + ":5432/postgres", "postgres", "postgres");
    readStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection.prepareStatement(
                    "SELECT * FROM events WHERE event_sequence_id > ? AND MOD(aggregate_id, ?) = ? ORDER BY event_sequence_id");
              } catch (SQLException e) {
                e.printStackTrace();
                return null;
              }
            });
  }

  @Override
  public void run() {
    try {
      while (true) {
        try {
          readStatement.get().setLong(1, latestSequenceId.get());
          readStatement.get().setLong(2, poolSize);
          readStatement.get().setLong(3, index.get());
          try (ResultSet resultSet = readStatement.get().executeQuery()) {
            while (resultSet.next()) {
              timer
                  .get()
                  .record(
                      () -> {
                        try {
                          COMMAND_DTO_EVENT.set(
                              READER.readValue(resultSet.getString(resultSet.findColumn("data"))));
                          ChronicleMapPersister.persistToCountsMap(
                              COMMAND_DTO_EVENT.get().getId(), COMMAND_DTO_EVENT.get(), counts);
                          latestSequenceId.set(
                              resultSet.getLong(resultSet.findColumn("event_sequence_id")));
                          System.out.println(
                              "update state for "
                                  + COMMAND_DTO_EVENT.get().getId()
                                  + " from event store, event id: "
                                  + resultSet.getLong(resultSet.findColumn("event_sequence_id")));
                        } catch (SQLException | JsonProcessingException ex) {
                          ex.printStackTrace();
                        }
                      });
            }
          } catch (SQLException e) {
            e.printStackTrace();
          }
          Thread.sleep(1000);
        } catch (SQLException | InterruptedException ex) {
          ex.printStackTrace();
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
