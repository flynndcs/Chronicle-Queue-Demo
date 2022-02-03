package com.flynndcs.app.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

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
