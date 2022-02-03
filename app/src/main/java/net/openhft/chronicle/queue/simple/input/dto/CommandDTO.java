package net.openhft.chronicle.queue.simple.input.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.openhft.chronicle.queue.simple.input.http.CommandHandler;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.util.UUID;

public class CommandDTO extends SelfDescribingMarshallable {
  @JsonProperty("action")
  private String action;

  @JsonProperty("id")
  private long id;

  @JsonProperty("value")
  private String value;

  public CommandDTO() {}

  public String getAction() {
    return this.action;
  }

  public long getId() {
    return this.id;
  }

  public String getValue() {
    return this.value;
  }
}
