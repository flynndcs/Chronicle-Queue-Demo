package com.flynndcs.app.state;

import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleMapPersister {
  private static final ThreadLocal<Long> count = ThreadLocal.withInitial(() -> 0L);

  public static void persistToCountsMap(
      Long key, CommandDTO dto, ChronicleMap<Long, Long> countsMap) {
    if ("add".equalsIgnoreCase(dto.getAction())) {
      if (countsMap.getUsing(key, count.get()) == null) {
        countsMap.put(key, dto.getQuantity());
      } else {
        count.set(countsMap.getUsing(key, count.get()) + dto.getQuantity());
        countsMap.put(key, count.get());
      }
    } else {
      countsMap.remove(key);
    }
  }
}
