package com.flynndcs.app.eventstore;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

import java.nio.ByteBuffer;

public class ItemEventListener implements Runnable {
  private static String DB_HOSTNAME;
  private static ChronicleMap<LongValue, CharSequence> values;
  private static ChronicleMap<ByteBuffer, Boolean> events;
  private static PrometheusMeterRegistry metrics;

  public ItemEventListener(
      String dbHost,
      ChronicleMap<LongValue, CharSequence> valuesMap,
      ChronicleMap<ByteBuffer, Boolean> eventsMap,
      PrometheusMeterRegistry metricsRegistry) {
    DB_HOSTNAME = dbHost;
    values = valuesMap;
    events = eventsMap;
    metrics = metricsRegistry;
  }

  @Override
  public void run() {
    //listen to postgres server
    //for each recent event from postgres and handled events in eventsMap
    //if earliest postgres event is after newest handled event, apply all from postgres
    //if not, start with earliest from both and compare each pair and apply in order.
  }
}
