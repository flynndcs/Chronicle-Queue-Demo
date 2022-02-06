package com.flynndcs.app.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class CommandDTO extends SelfDescribingMarshallable {
  @JsonProperty("id")
  private Long id;

  @JsonProperty("action")
  private String action;

  @JsonProperty("quantity")
  private Long quantity;

  public CommandDTO() {}

  public String getAction() {
    return this.action;
  }

  public Long getId() {
    return this.id;
  }

  public Long getQuantity() {
    return this.quantity;
  }
}
