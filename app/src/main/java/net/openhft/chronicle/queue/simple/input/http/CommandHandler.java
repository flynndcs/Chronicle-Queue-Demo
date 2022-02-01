package net.openhft.chronicle.queue.simple.input.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.simple.input.domain.CommandValidator;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CommandHandler implements HttpHandler {
  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
  private static final ObjectWriter WRITER = new ObjectMapper().writerFor(CommandDTO.class);
  private final ExcerptAppender APPENDER;
  private static final StringBuilder sb = new StringBuilder();
  private static CommandDTO COMMAND_DTO = new CommandDTO();
  private static boolean success = false;
  private static final String INVALID_COMMAND = "Invalid command.";
  private final Connection connection;
  private PreparedStatement statement;

  public CommandHandler(ExcerptAppender appender, Connection connection) {
    this.APPENDER = appender;
    this.connection = connection;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      success = handlePost(exchange);
    } else {
      System.err.println("Unsupported HTTP action:" + exchange.getRequestMethod());
      return;
    }

    if (success) {
      sb.append("Command received.");
      sb.append(System.lineSeparator());
      exchange.sendResponseHeaders(200, sb.length());
    } else {
      sb.append("Command not registered.");
      sb.append(System.lineSeparator());
      exchange.sendResponseHeaders(400, sb.length());
    }
    exchange.getResponseBody().write(sb.toString().getBytes(UTF_8));
    exchange.getRequestBody().close();
    sb.setLength(0);
  }

  public boolean handlePost(HttpExchange exchange) {
    try {
      COMMAND_DTO = READER.readValue(exchange.getRequestBody());
      if (CommandValidator.isValid(COMMAND_DTO)) {
        APPENDER.writeDocument(wire -> COMMAND_DTO.writeMarshallable(wire));

        // write same command to event store
        statement =
            connection.prepareStatement("INSERT INTO event (event, written_at) VALUES (?, ?)");
        statement.setString(1, WRITER.writeValueAsString(COMMAND_DTO));
        statement.setLong(2, ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()));
        statement.executeUpdate();
        statement.close();

        return true;
      } else {
        System.err.println(INVALID_COMMAND);
        return false;
      }
    } catch (IOException | SQLException e) {
      e.printStackTrace();
      return false;
    }
  }
}
