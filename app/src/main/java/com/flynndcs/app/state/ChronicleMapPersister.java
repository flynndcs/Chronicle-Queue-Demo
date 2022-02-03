package com.flynndcs.app.state;

import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

public class ChronicleMapPersister {
  public static void persistToValuesMap(
          LongValue key, CommandDTO dto, ChronicleMap<LongValue, CharSequence> valueMap) {
    if ("UPSERT".equalsIgnoreCase(dto.getAction())) {
      valueMap.put(key, dto.getValue());
    } else {
      valueMap.remove(key);
    }
  }
}
