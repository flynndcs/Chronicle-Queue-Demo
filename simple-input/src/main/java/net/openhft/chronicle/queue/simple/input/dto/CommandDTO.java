package net.openhft.chronicle.queue.simple.input.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

public class CommandDTO implements Marshallable {
  @JsonProperty("action")
  private String action;

  @JsonProperty("id")
  private long id;

  @JsonProperty("value")
  private String value;

  public String getAction() {
    return this.action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public long getId() {
    return this.id;
  }

  public String getValue() {
    return this.value;
  }

  public void setId(long id) {
    this.id = id;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public void writeMarshallable(WireOut wire) {
    wire.write("action").text(action).write("id").int64(id).write("value").text(value);
  }

  @Override
  public void readMarshallable(WireIn wire) throws IllegalStateException {
    wire.read("action")
        .text(this, CommandDTO::setAction)
        .read("id")
        .int64(this, CommandDTO::setId)
        .read("value")
        .text(this, CommandDTO::setValue);
  }
}
