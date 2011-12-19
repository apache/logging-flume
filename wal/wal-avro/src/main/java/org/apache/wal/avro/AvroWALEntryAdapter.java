package org.apache.wal.avro;

import org.apache.wal.WALEntry;

public class AvroWALEntryAdapter implements WALEntry {

  private AvroWALEntry entry;

  public AvroWALEntryAdapter(AvroWALEntry entry) {
    this.entry = entry;
  }

  @Override
  public String toString() {
    return "AvroWALEntryAdapter { entry:" + entry + " }";
  }

  public AvroWALEntry getEntry() {
    return entry;
  }

}
