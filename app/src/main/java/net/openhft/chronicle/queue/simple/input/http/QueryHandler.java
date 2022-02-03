package net.openhft.chronicle.queue.simple.input.http;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.values.Values;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.time.Duration;

public class QueryHandler extends AbstractHandler {
  private static ChronicleMap<LongValue, CharSequence> VALUE_MAP;
  private static final LongValue longValue = Values.newHeapInstance(LongValue.class);

  private static final String UNSUPPORTED_HTTP_MESSAGE = "Unsupported HTTP action.";
  private static final String NO_QUERY_RESULT = "No result for query.";
  private static Timer TIMER;

  public QueryHandler(
      ChronicleMap<LongValue, CharSequence> valueMap, PrometheusMeterRegistry metrics) {
    VALUE_MAP = valueMap;
    TIMER =
        Timer.builder("query_hist")
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
    respond(response, tryHandleGet(request));
  }

  private String tryHandleGet(HttpServletRequest request) {
    if ("GET".equalsIgnoreCase(request.getMethod())) {
      return TIMER.record(() -> handleGet(request.getQueryString()));
    } else {
      System.err.println(UNSUPPORTED_HTTP_MESSAGE);
      return null;
    }
  }

  private String handleGet(String queryParams) {
    longValue.setValue(Long.parseLong((queryParams.split("&")[0].split("=")[1])));
    StringBuilder SB = new StringBuilder();
    VALUE_MAP.getUsing(longValue, SB);
    if (!SB.toString().isEmpty()) {
      return SB.toString();
    } else {
      return null;
    }
  }

  private void respond(HttpServletResponse response, String queryResponse) throws IOException {
    response.setContentType("text/plain; charset=utf-8");
    response.setCharacterEncoding("UTF-8");
    if (queryResponse != null) {
      response.setStatus(200);
    } else {
      queryResponse = NO_QUERY_RESULT;
      response.setStatus(400);
    }
    response.getWriter().print(queryResponse);
    response.getWriter().flush();
  }
}
