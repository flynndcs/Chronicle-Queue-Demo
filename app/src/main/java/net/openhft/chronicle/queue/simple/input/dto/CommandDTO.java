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

  public CommandDTO(String action, long id, String value) {
    this.action = action;
    this.id = id;
    this.value = value;
  }

  public CommandDTO() {}

  public String getAction() {
    return this.action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public long getId() {
    return this.id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getValue() {
    return this.value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
