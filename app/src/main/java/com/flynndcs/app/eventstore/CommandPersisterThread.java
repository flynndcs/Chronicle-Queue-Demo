package com.flynndcs.app.eventstore;

import com.flynndcs.app.dto.CommandDTO;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;

import java.sql.SQLException;
import java.util.Objects;

public class CommandPersisterThread implements Runnable {
  private static long poolSize;
  private ThreadLocal<Long> index;
  private ThreadLocal<ExcerptTailer> tailer;
  private ThreadLocal<EventPersister> persister;
  private ThreadLocal<CommandDTO> dto;

  public CommandPersisterThread(
      long numThreads,
      long threadIndex,
      EventPersister eventPersister,
      ExcerptTailer excerptTailer) {
    // create thread local variables since events are sharded across the pool of command persisters
    poolSize = numThreads;
    index = ThreadLocal.withInitial(() -> threadIndex);
    persister = ThreadLocal.withInitial(() -> eventPersister);
    tailer = ThreadLocal.withInitial(() -> excerptTailer);
    dto = ThreadLocal.withInitial(CommandDTO::new);
  }

  @Override
  public void run() {
    while (true) {
      while (true) {
        // try to read from queue
        try (final DocumentContext dc = tailer.get().readingDocument()) {
          if (dc.isPresent()) {
            // read the document in the queue into the thread local dto object
            dto.get().readMarshallable(Objects.requireNonNull(dc.wire()));
            // if this dto's id partitions into our bucket, persist it to the event store
            if (dto.get().getId() % poolSize == index.get()) {
              // persist
              if (!persister.get().onCommand(dto.get())) {
                System.out.println("persisting command failed, please retry.");
              }
            }
          } else {
            break;
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
