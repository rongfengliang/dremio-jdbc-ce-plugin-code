package com.dremio.exec.store.jdbc.rel2sql.utilities;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class AliasSanitizer {
   private static final Set<String> ALIAS_BLACKLIST = new HashSet(Arrays.asList("*"));
   private static final String PREFIX = "__";

   private AliasSanitizer() {
   }

   public static String sanitizeAlias(String name) {
      if (ALIAS_BLACKLIST.contains(name)) {
         name = "__" + name;
      }

      return name;
   }
}
