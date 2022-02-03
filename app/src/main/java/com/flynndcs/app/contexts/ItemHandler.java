package com.flynndcs.app.contexts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import com.flynndcs.app.state.ChronicleMapPersister;
import com.flynndcs.app.eventstore.MessageConsumer;
import com.flynndcs.app.dto.CommandDTO;
import com.flynndcs.app.dto.CommandDTOValidator;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.binary;

public class ItemHandler extends AbstractHandler {
  private static ChronicleMap<LongValue, CharSequence> valueMap;
  private static final LongValue longValue = Values.newHeapInstance(LongValue.class);
  private static final ThreadLocal<CommandDTO> commandDto =
      ThreadLocal.withInitial(() -> Values.newHeapInstance(CommandDTO.class));
  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
  private static final LongValue key = Values.newHeapInstance(LongValue.class);
  private static MessageConsumer SENDER;

  private static final String UNSUPPORTED_HTTP_MESSAGE = "Unsupported HTTP action.";
  private static Timer GET_TIMER;
  private static Timer POST_TIMER;

  public ItemHandler(
      ChronicleMap<LongValue, CharSequence> valueMap, PrometheusMeterRegistry metrics) {
    ItemHandler.valueMap = valueMap;
    SENDER = binary("queue").build().acquireAppender().methodWriter(MessageConsumer.class);
    GET_TIMER =
        Timer.builder("query")
            .publishPercentiles(0.5, 0.9, 0.99)
            .publishPercentileHistogram()
            .minimumExpectedValue(Duration.ofNanos(1000))
            .maximumExpectedValue(Duration.ofSeconds(2))
            .register(metrics);
    POST_TIMER =
        Timer.builder("command")
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
    baseRequest.setHandled(true);
    respond(response, tryHandle(request));
  }

  private String tryHandle(HttpServletRequest request) {
    if ("GET".equalsIgnoreCase(request.getMethod())) {
      return GET_TIMER.record(() -> handleGet(request.getQueryString()));
    } else if ("POST".equalsIgnoreCase(request.getMethod())) {
      return POST_TIMER.record(() -> handlePost(request));
    } else {
      System.err.println(UNSUPPORTED_HTTP_MESSAGE);
      return null;
    }
  }

  private String handleGet(String queryParams) {
    longValue.setValue(Long.parseLong((queryParams.split("&")[0].split("=")[1])));
    StringBuilder SB = new StringBuilder();
    valueMap.getUsing(longValue, SB);
    if (!SB.toString().isEmpty()) {
      return SB.toString();
    } else {
      return "Value not found.";
    }
  }

  public String handlePost(HttpServletRequest request) {
    try {
      commandDto.set(READER.readValue(request.getInputStream(), CommandDTO.class));
      if (CommandDTOValidator.isValid(commandDto.get())) {
        key.setValue(commandDto.get().getId());
        ChronicleMapPersister.persistToValuesMap(key, commandDto.get(), valueMap);
        SENDER.onCommand(commandDto.get());
        return "Command received";
      } else {
        return "Command not received";
      }
    } catch (IOException | SQLException e) {
      e.printStackTrace();
      return "Command not received";
    }
  }

  private void respond(HttpServletResponse response, String queryResponse) throws IOException {
    response.setContentType("text/plain; charset=utf-8");
    response.setCharacterEncoding("UTF-8");
    if (queryResponse != null) {
      response.setStatus(200);
    } else {
      response.setStatus(400);
    }
    response.getWriter().print(queryResponse);
    response.getWriter().flush();
  }
}
