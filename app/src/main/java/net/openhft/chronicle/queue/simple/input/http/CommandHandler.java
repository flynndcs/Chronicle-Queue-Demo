package net.openhft.chronicle.queue.simple.input.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.Histogram;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.simple.input.MessageConsumer;
import net.openhft.chronicle.queue.simple.input.domain.CommandValidator;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class CommandHandler extends AbstractHandler {
  private static PrometheusMeterRegistry METRICS;
  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
  private static ChronicleMap<LongValue, CharSequence> VALUE_MAP;
  private static MessageConsumer CONSUMER;
  private static CommandDTO COMMAND_DTO = new CommandDTO();
  private static boolean success = false;
  private static String response = "";
  private static final String INVALID_COMMAND = "Invalid command.";
  private static final LongValue key = Values.newHeapInstance(LongValue.class);
  private static PrintWriter writer;

  public CommandHandler(
      ChronicleMap<LongValue, CharSequence> map, PrometheusMeterRegistry metrics) {
    VALUE_MAP = map;
    CONSUMER = binary("queue").build().acquireAppender().methodWriter(MessageConsumer.class);
    METRICS = metrics;
  }

  @Override
  public void handle(
      String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    Timer timer = METRICS.timer("post_command");

    response.setContentType("text/plain; charset=utf-8");
    response.setCharacterEncoding("UTF-8");
    if ("POST".equalsIgnoreCase(request.getMethod())) {
      timer.record(() ->{
        success = handlePost(request);
      });
    } else {
      System.err.println("Unsupported HTTP action:" + request.getMethod());
      return;
    }

    if (success) {
      CommandHandler.response = "Command received." + System.lineSeparator();
      response.setStatus(HttpServletResponse.SC_OK);
    } else {
      CommandHandler.response = "Command not registered" + System.lineSeparator();
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
    writer = response.getWriter();
    writer.print(CommandHandler.response);
    baseRequest.setHandled(true);
    writer.flush();
  }

  public boolean handlePost(HttpServletRequest request) {
    try {
      COMMAND_DTO = READER.readValue(request.getInputStream(), CommandDTO.class);
      if (CommandValidator.isValid(COMMAND_DTO)) {
        key.setValue(COMMAND_DTO.getId());
        VALUE_MAP.put(key, COMMAND_DTO.getValue());
        System.out.println("state updated");
        CONSUMER.onMessage(COMMAND_DTO);
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
