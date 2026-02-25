package com.cdc.destination.iceberg;

import com.cdc.config.DestinationConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IcebergCatalogFactory {
    private static final Logger log = LoggerFactory.getLogger(IcebergCatalogFactory.class);

    public static Catalog create(DestinationConfig config) {
        String catalogType = config.getCatalogType();
        log.info("Creating Iceberg catalog of type: {}", catalogType);

        return switch (catalogType.toLowerCase()) {
            case "hadoop" -> createHadoopCatalog(config);
            default -> throw new UnsupportedOperationException(
                "Catalog type not yet supported: " + catalogType +
                ". Currently supported: hadoop");
        };
    }

    private static HadoopCatalog createHadoopCatalog(DestinationConfig config) {
        Configuration hadoopConf = new Configuration();
        String warehouse = config.getWarehouse();
        if (warehouse == null || warehouse.isEmpty()) {
            warehouse = config.getBasePath();
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);

        if (config.getCatalogProperties() != null) {
            properties.putAll(config.getCatalogProperties());
        }

        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize("cdc_catalog", properties);

        log.info("Hadoop catalog created with warehouse: {}", warehouse);
        return catalog;
    }
}
