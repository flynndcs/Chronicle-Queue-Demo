package com.flynndcs.app.eventstore;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class MessagePersister implements MessageConsumer {
  private static PreparedStatement statement;
  private static ObjectWriter writer;

  public MessagePersister(PreparedStatement statement, ObjectWriter writer) {
    MessagePersister.statement = statement;
    MessagePersister.writer = writer;
  }

  @Override
  public void onCommand(CommandDTO dto) throws JsonProcessingException, SQLException {
    statement.setObject(1, Uuids.timeBased());
    statement.setString(2, writer.writeValueAsString(dto));
    statement.setLong(3, ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()));
    statement.executeUpdate();
  }
}
