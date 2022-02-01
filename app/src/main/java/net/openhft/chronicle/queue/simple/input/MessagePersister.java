package net.openhft.chronicle.queue.simple.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

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
  public void onMessage(CommandDTO dto) throws JsonProcessingException, SQLException {
    statement.setString(1, writer.writeValueAsString(dto));
    statement.setLong(2, ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()));
    statement.executeUpdate();
  }
}
