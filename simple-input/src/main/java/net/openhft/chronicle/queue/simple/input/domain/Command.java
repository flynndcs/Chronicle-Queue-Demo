package net.openhft.chronicle.queue.simple.input.domain;
public class Command {
  public static final String UPSERT = "upsert";
  public static final String DELETE = "delete";
  private String action;
  private long id;
  private String value;

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
