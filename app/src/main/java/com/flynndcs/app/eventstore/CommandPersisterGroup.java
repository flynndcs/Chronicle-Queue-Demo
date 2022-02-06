package com.flynndcs.app.eventstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CommandPersisterGroup {
  private static List<ExecutorService> executors = new ArrayList<>();
  private final ObjectWriter WRITER = new ObjectMapper().writerFor(CommandDTO.class);
  private final String dbHostname;
  private final SingleChronicleQueue queue;

  public CommandPersisterGroup(String dbHost, int poolSize, SingleChronicleQueue commandQueue) {
    dbHostname = dbHost;
    queue = commandQueue;
    for (int i = 0; i < poolSize; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
  }

  public void start() throws SQLException {
    long i = 0L;
    for (ExecutorService service : executors) {
      service.submit(
          new CommandPersisterThread(
              executors.size(), i, new EventPersister(dbHostname, WRITER), queue.createTailer()));
      i++;
    }
  }
}
