package net.openhft.chronicle.queue.simple.input.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryHandler implements HttpHandler {
  private final Map<Long, String> ITEMS_MAP;
  private static long NANOS_START = 0;
  private static String QUERY_RESPONSE = "";
  private static final Map<String, String> QUERY_PARAM_MAP = new HashMap<>();
  private static String[] NAME_VALUE_PAIRS = new String[2];
  private static String NAME = "";
  private static String VALUE = "";
  private static final StringBuilder SB = new StringBuilder();

  private static final String UNSUPPORTED_HTTP_MESSAGE = "Unsupported HTTP action.";
  private static final String QUERY_RESULT = "Query result:";
  private static final String NO_QUERY_RESULT = "No result for query.";
  private static final String MICROS_ELAPSED = "Micros elapsed: ";

  public QueryHandler(Map<Long, String> itemsMap) {
    this.ITEMS_MAP = itemsMap;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    NANOS_START = System.nanoTime();
    QUERY_RESPONSE = tryHandleGet(exchange);
    respond(exchange, QUERY_RESPONSE, NANOS_START);
  }

  private String tryHandleGet(HttpExchange exchange) {
    if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      return handleGet(exchange.getRequestURI());
    } else {
      System.err.println(UNSUPPORTED_HTTP_MESSAGE);
      return null;
    }
  }

  private String handleGet(URI uri) {
    NAME_VALUE_PAIRS = uri.getQuery().split("&");
    for (String param : NAME_VALUE_PAIRS) {
      NAME = param.split("=")[0];
      VALUE = param.split("=")[1];
      QUERY_PARAM_MAP.put(NAME, VALUE);
    }
    return ITEMS_MAP.get(Long.parseLong(QUERY_PARAM_MAP.get("id")));
  }

  private void respond(HttpExchange exchange, String queryResponse, long nanos) throws IOException {
    if (queryResponse != null) {
      exchange.sendResponseHeaders(200, SB.length());
      SB.append(QUERY_RESULT);
      SB.append(System.lineSeparator());
      SB.append(queryResponse);
      SB.append(System.lineSeparator());
    } else {
      SB.append(NO_QUERY_RESULT);
      exchange.sendResponseHeaders(400, SB.length());
    }
    exchange.getResponseBody().write(SB.toString().getBytes(UTF_8));
    System.out.println(MICROS_ELAPSED + ((System.nanoTime() - nanos) / 1000));
    exchange.getResponseBody().close();
    SB.setLength(0);
  }
}
