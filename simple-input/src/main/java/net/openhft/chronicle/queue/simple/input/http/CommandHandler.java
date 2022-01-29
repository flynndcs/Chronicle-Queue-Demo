package net.openhft.chronicle.queue.simple.input.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommandHandler implements HttpHandler {
  private static final ObjectMapper mapper = new ObjectMapper();
  private ExcerptAppender appender;

  public CommandHandler(ExcerptAppender appender) {
    this.appender = appender;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    AtomicBoolean success = new AtomicBoolean(false);
    if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
      success.set(handlePost(exchange));
    } else {
      System.err.println("Unsupported HTTP action.");
      return;
    }

    StringBuilder sb = new StringBuilder();
    if (success.get()) {
      sb.append("Command received.");
      exchange.sendResponseHeaders(200, sb.length());
    } else {
      sb.append("Command not registered.");
      exchange.sendResponseHeaders(400, sb.length());
    }
    OutputStream stream = exchange.getResponseBody();
    stream.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    stream.flush();
    stream.close();
  }

  public boolean handlePost(HttpExchange exchange) {
    try {
      appender.writeText(mapper.readValue(exchange.getRequestBody(), CommandDTO.class).toCommand());
      return true;
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }
}
