package com.flynndcs.app.contexts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.map.ChronicleMap;
import com.flynndcs.app.dto.CommandDTO;
import com.flynndcs.app.dto.CommandDTOValidator;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.time.Duration;

public class TenantHandler extends AbstractHandler {
  private static ChronicleMap<Long, Long> countsMap;
  private static final ThreadLocal<Long> longValue = ThreadLocal.withInitial(() -> 0L);
  private static final ThreadLocal<CommandDTO> commandDto =
      ThreadLocal.withInitial(() -> Values.newHeapInstance(CommandDTO.class));
  private static final ObjectReader READER = new ObjectMapper().readerFor(CommandDTO.class);
  private static ThreadLocal<ExcerptAppender> APPENDER;

  private static final String UNSUPPORTED_HTTP_MESSAGE = "Unsupported HTTP action.";
  private static Timer GET_TIMER;
  private static Timer POST_TIMER;

  public TenantHandler(
      ChronicleMap<Long, Long> map, PrometheusMeterRegistry metrics, SingleChronicleQueue queue) {
    APPENDER = ThreadLocal.withInitial(queue::acquireAppender);
    countsMap = map;
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
    // get count of items for this aggregate id from application state
    longValue.set(countsMap.get(Long.parseLong(queryParams.split("&")[0].split("=")[1])));
    if (longValue.get() != null) {
      return longValue.get().toString() + System.lineSeparator();
    } else {
      return "Count not found: " + longValue.get() + System.lineSeparator();
    }
  }

  public String handlePost(HttpServletRequest request) {
    try {
      // get command from json and validate
      commandDto.set(READER.readValue(request.getInputStream(), CommandDTO.class));
      if (CommandDTOValidator.isValid(commandDto.get())) {
        // append to end of memory mapped file queue on disk
        APPENDER.get().writeDocument(commandDto.get());
        return "Command received" + System.lineSeparator();
      } else {
        return "Command not received" + System.lineSeparator();
      }
    } catch (IOException e) {
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
