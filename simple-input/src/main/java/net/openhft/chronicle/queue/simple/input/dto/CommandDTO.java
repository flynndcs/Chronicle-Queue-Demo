package net.openhft.chronicle.queue.simple.input.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CommandDTO {
  @JsonProperty("action")
  private String action;

  @JsonProperty("id")
  private Integer id;

  @JsonProperty("value")
  private String value;

  public String toCommand(){
    return this.action + " " + this.id + " " + this.value;
  }
}
