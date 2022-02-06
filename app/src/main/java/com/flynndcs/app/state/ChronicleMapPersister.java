package com.flynndcs.app.state;

import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleMapPersister {
  private static final ThreadLocal<Long> prevCount = ThreadLocal.withInitial(() -> 0L);
  private static final ThreadLocal<Long> newCount = ThreadLocal.withInitial(() -> 0L);

  public static void persistToCountsMap(CommandDTO dto, ChronicleMap<Long, Long> countsMap) {
    if (!countsMap.containsKey(dto.getId())) {
      countsMap.put(dto.getId(), dto.getQuantity());
      return;
    }
    countsMap.getUsing(dto.getId(), prevCount.get());
    if ("add".equalsIgnoreCase(dto.getAction())) {
      newCount.set(dto.getQuantity() + prevCount.get());
      countsMap.put(dto.getId(), newCount.get());
    } else {
      // something
    }
  }
}
