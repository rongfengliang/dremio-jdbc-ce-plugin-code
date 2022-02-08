package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractArpConf<T extends JdbcConf<T>> extends JdbcConf<T> {
   private static final Logger logger = LoggerFactory.getLogger(AbstractArpConf.class);

   protected static <T extends ArpDialect> T loadArpFile(String pathToArpFile, Function<ArpYaml, T> dialectConstructor) {
      ArpDialect dialect;
      try {
         ArpYaml yaml = ArpYaml.createFromFile(pathToArpFile);
         dialect = (ArpDialect)dialectConstructor.apply(yaml);
      } catch (Exception var4) {
         dialect = null;
         logger.error("Error creating dialect from ARP file {}.", pathToArpFile, var4);
      }

      return dialect;
   }
}
