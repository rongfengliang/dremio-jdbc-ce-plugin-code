package com.dremio.exec.store.jdbc.dialect;

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;

public class WarningContextList implements Iterable<WarningContext> {
   private final List<WarningContext> list = Lists.newArrayList();

   public WarningContextList add(String key, String value) {
      this.list.add(new WarningContext(key, value));
      return this;
   }

   public Iterator<WarningContext> iterator() {
      return this.list.iterator();
   }
}
