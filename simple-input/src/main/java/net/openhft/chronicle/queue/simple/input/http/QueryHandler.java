package net.openhft.chronicle.queue.simple.input.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryHandler implements HttpHandler {
  private final Map<Integer, String> itemsMap;

  public QueryHandler(Map<Integer, String> itemsMap) {
    this.itemsMap = itemsMap;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    AtomicBoolean queryResultFound = new AtomicBoolean(false);
    String queryResponse;
    if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      queryResponse = handleGet(exchange);
      if (queryResponse != null) {
        queryResultFound.set(true);
      }
    } else {
      System.err.println("Unsupported HTTP action.");
      return;
    }

    StringBuilder sb = new StringBuilder();

    OutputStream stream = exchange.getResponseBody();
    if (queryResultFound.get()) {
      exchange.sendResponseHeaders(200, sb.length());
      sb.append("Query Result:");
      sb.append(System.lineSeparator());
      sb.append(queryResponse);
    } else {
      sb.append("No result for query.");
      exchange.sendResponseHeaders(400, sb.length());
    }
    stream.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    stream.flush();
    stream.close();
  }

  private String handleGet(HttpExchange exchange) {
    Map<String, String> queryParamMap = new HashMap<>();
    URI uri = exchange.getRequestURI();
    String[] nameValuePairs = uri.getQuery().split("&");
    for (String param : nameValuePairs) {
      String name = param.split("=")[0];
      String value = param.split("=")[1];
      queryParamMap.put(name, value);
    }
    Integer id = Integer.parseInt(queryParamMap.get("id"));
    return itemsMap.get(id);
  }
}
