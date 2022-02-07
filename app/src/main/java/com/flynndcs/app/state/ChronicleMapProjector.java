package com.flynndcs.app.state;

import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleMapProjector {
  public void projectToCountsMap(CommandDTO dto, ChronicleMap<Long, Long> countsMap) {
    if (!countsMap.containsKey(dto.getId()) && "add".equalsIgnoreCase(dto.getAction())) {
      countsMap.put(dto.getId(), dto.getQuantity());
      return;
    }
    if ("add".equalsIgnoreCase(dto.getAction())) {
      synchronized (this) {
        countsMap.put(dto.getId(), countsMap.get(dto.getId()) + dto.getQuantity());
      }
    } else {
      // something
    }
  }
}
