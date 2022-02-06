package com.flynndcs.app.eventstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EventPersister implements MessageConsumer {
  private static PreparedStatement readAggregateStatement;
  private static PreparedStatement updateAggregateStatement;
  private static PreparedStatement insertEventStatement;
  private static PreparedStatement insertAggregateStatement;
  private static Connection connection;
  private static ResultSet rs;
  private static ObjectWriter writer;

  public EventPersister(Connection dbConnection, ObjectWriter objWriter) {
    connection = dbConnection;
    try {
      readAggregateStatement =
          connection.prepareStatement(
              "SELECT * FROM aggregates WHERE aggregate_id = ? ORDER BY version DESC LIMIT 1");
      updateAggregateStatement =
          connection.prepareStatement(
              "UPDATE aggregates set version = version + 1 where aggregate_id = ? AND version = ?");
      insertEventStatement =
          connection.prepareStatement(
              "INSERT INTO events (aggregate_id, data, version) VALUES (?, ?, ?)");
      insertAggregateStatement =
          connection.prepareStatement(
              "INSERT INTO aggregates (aggregate_id, version) VALUES (?, ?)");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    writer = objWriter;
  }

  @Override
  public void onCommand(CommandDTO dto) throws SQLException {
    readAggregateStatement.setObject(1, dto.getId());
    rs = readAggregateStatement.executeQuery();
    int currentVersion;
    if (rs.isBeforeFirst()) {
      rs.next();
      currentVersion = rs.getInt(rs.findColumn("version"));

      connection.setAutoCommit(false);
      updateAggregateStatement.setObject(1, dto.getId());
      updateAggregateStatement.setInt(2, currentVersion);
      if (updateAggregateStatement.executeUpdate() == 0) {
        connection.rollback();
      }
    } else {
      connection.setAutoCommit(false);
      insertAggregateStatement.setObject(1, dto.getId());
      insertAggregateStatement.setInt(2, 0);
      insertAggregateStatement.executeUpdate();
      currentVersion = -1;
    }

    try {
      insertEventStatement.setObject(1, dto.getId());
      insertEventStatement.setString(2, writer.writeValueAsString(dto));
      insertEventStatement.setInt(3, currentVersion + 1);
      insertEventStatement.executeUpdate();
    } catch (JsonProcessingException e) {
      connection.rollback();
      System.out.println(
          "persisting to event store for "
              + dto.getAction()
              + " "
              + dto.getQuantity()
              + " failed. rolling back transaction.");
    }
    connection.commit();
    System.out.println("committed records to event store.");
    connection.setAutoCommit(true);
  }
}
