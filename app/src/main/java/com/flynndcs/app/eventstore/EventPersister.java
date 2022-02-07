package com.flynndcs.app.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EventPersister {
  private final ThreadLocal<PreparedStatement> readAggregateStatement;
  private final ThreadLocal<PreparedStatement> updateAggregateStatement;
  private final ThreadLocal<PreparedStatement> insertEventStatement;
  private final ThreadLocal<PreparedStatement> insertAggregateStatement;
  private final ThreadLocal<Connection> connection;
  private final ThreadLocal<ResultSet> rs;
  private final ThreadLocal<ObjectWriter> writer;

  public EventPersister(String dbHost, ObjectWriter objWriter) {
    // each instance has its own connection to event store
    connection =
        ThreadLocal.withInitial(
            () -> {
              try {
                return DriverManager.getConnection(
                    "jdbc:postgresql://" + dbHost + ":5432/postgres", "postgres", "postgres");
              } catch (SQLException e) {
                e.printStackTrace();

                return null;
              }
            });
    // find the most recent aggregate denoted by its version
    readAggregateStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection
                    .get()
                    .prepareStatement(
                        "SELECT * FROM aggregates WHERE aggregate_id = ? ORDER BY version DESC LIMIT 1");
              } catch (SQLException e) {
                e.printStackTrace();

                return null;
              }
            });
    // update the aggregate's version in the event store to recognize that an event occurred
    updateAggregateStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection
                    .get()
                    .prepareStatement(
                        "UPDATE aggregates set version = version + 1 where aggregate_id = ? AND version = ?");
              } catch (SQLException e) {
                e.printStackTrace();

                return null;
              }
            });
    // persist the event itself in the event store
    insertEventStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection
                    .get()
                    .prepareStatement(
                        "INSERT INTO events (aggregate_id, data, version) VALUES (?, ?, ?)");
              } catch (SQLException e) {
                e.printStackTrace();

                return null;
              }
            });
    // add a new aggregate if the id is not present in the event store yet
    insertAggregateStatement =
        ThreadLocal.withInitial(
            () -> {
              try {
                return connection
                    .get()
                    .prepareStatement(
                        "INSERT INTO aggregates (aggregate_id, version) VALUES (?, ?)");
              } catch (SQLException e) {
                e.printStackTrace();
                return null;
              }
            });
    writer = ThreadLocal.withInitial(() -> objWriter);
    rs = ThreadLocal.withInitial(() -> null);
  }

  // optimistic concurrency controls
  public boolean onCommand(CommandDTO dto) throws SQLException {
    // get most recent version of aggregate and increment
    readAggregateStatement.get().setLong(1, dto.getId());
    rs.set(readAggregateStatement.get().executeQuery());
    int currentVersion;
    if (rs.get().isBeforeFirst()) {
      rs.get().next();
      currentVersion = rs.get().getInt(rs.get().findColumn("version"));

      // start the optimistic concurrency transaction
      connection.get().setAutoCommit(false);
      updateAggregateStatement.get().setLong(1, dto.getId());
      updateAggregateStatement.get().setInt(2, currentVersion);
      if (updateAggregateStatement.get().executeUpdate() == 0) {
        // this failed because the aggregate in question was concurrently incremented from another
        // transaction that was further along. fail
        connection.get().rollback();
        System.out.println(
            "persisting to event store for "
                + dto.getAction()
                + " "
                + dto.getQuantity()
                + " failed. rolling back transaction.");
        connection.get().setAutoCommit(true);
        return false;
      }
    } else {
      // if no aggregate found, insert with version 0, start optimistic concurrency transaction
      connection.get().setAutoCommit(false);
      insertAggregateStatement.get().setLong(1, dto.getId());
      insertAggregateStatement.get().setInt(2, 0);
      insertAggregateStatement.get().executeUpdate();
      currentVersion = -1;
    }

    try {
      // insert event with reference to new aggregate version (incremented by 1)
      insertEventStatement.get().setLong(1, dto.getId());
      insertEventStatement.get().setString(2, writer.get().writeValueAsString(dto));
      insertEventStatement.get().setInt(3, currentVersion + 1);
      insertEventStatement.get().executeUpdate();
    } catch (JsonProcessingException e) {
      connection.get().rollback();
      System.out.println(
          "persisting to event store for "
              + dto.getAction()
              + " "
              + dto.getQuantity()
              + " failed. rolling back transaction.");
      connection.get().setAutoCommit(true);
      return false;
    }
    // commit this optimistic concurrency transaction
    connection.get().commit();
    connection.get().setAutoCommit(true);
    return true;
  }
}
