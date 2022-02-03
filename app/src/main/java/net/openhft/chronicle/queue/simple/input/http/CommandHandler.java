package net.openhft.chronicle.queue.simple.input.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.simple.input.MessageConsumer;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTOValidator;
import net.openhft.chronicle.queue.simple.input.dto.CommandDTO;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class CommandHandler extends AbstractHandler {
  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
  private static ChronicleMap<LongValue, CharSequence> valueMap;
  private static MessageConsumer SENDER;
  private static final ThreadLocal<CommandDTO> commandDto =
      ThreadLocal.withInitial(() -> Values.newHeapInstance(CommandDTO.class));
  private static boolean success = false;
  private static final ThreadLocal<CharSequence> handlerResponse =
      ThreadLocal.withInitial(() -> Values.newHeapInstance(CharSequence.class));
  private static final String INVALID_COMMAND = "Invalid command.";
  private static final LongValue key = Values.newHeapInstance(LongValue.class);
  private static Timer TIMER;

  public CommandHandler(
      ChronicleMap<LongValue, CharSequence> map, PrometheusMeterRegistry metrics) {
    valueMap = map;
    SENDER = binary("queue").build().acquireAppender().methodWriter(MessageConsumer.class);
    TIMER =
        Timer.builder("post_command_hist")
            .publishPercentiles(0.5, 0.9, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofSeconds(2))
            .register(metrics);
  }

  @Override
  public void handle(
      String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    response.setContentType("text/plain; charset=utf-8");
    response.setCharacterEncoding("UTF-8");
    if ("POST".equalsIgnoreCase(request.getMethod())) {
      TIMER.record(
          () -> {
            success = handlePost(request);
          });
    } else {
      System.err.println("Unsupported HTTP action:" + request.getMethod());
      return;
    }

    if (success) {
      handlerResponse.set("Command received.");
      response.setStatus(HttpServletResponse.SC_OK);
    } else {
      handlerResponse.set("Command not registered");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
    response.getWriter().print(handlerResponse.get());
    baseRequest.setHandled(true);
    response.getWriter().flush();
  }

  public boolean handlePost(HttpServletRequest request) {
    try {
      commandDto.set(READER.readValue(request.getInputStream(), CommandDTO.class));
      if (CommandDTOValidator.isValid(commandDto.get())) {
        key.setValue(commandDto.get().getId());
        valueMap.put(key, commandDto.get().getValue());
        SENDER.onCommand(commandDto.get());
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
