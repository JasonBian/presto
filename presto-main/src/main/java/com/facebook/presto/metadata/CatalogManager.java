/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.presto.ExceededCpuLimitException;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.server.PrestoServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Objects.requireNonNull;

public class CatalogManager
{
    private static final Logger log = Logger.get(CatalogManager.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();

    @Inject
    public CatalogManager(ConnectorManager connectorManager, CatalogManagerConfig config)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.<String>of()));
    }

    public CatalogManager(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        catalogsLoaded.set(true);

        //动态添加,修改,删除catalog
        new Thread(() -> {
            try {
                log.info("-- Catalog watcher thread start --");
                startCatalogWatcher(catalogConfigurationDir);
            } catch (Exception e) {
                log.info("startCatalogWatcher error," + e.getStackTrace());
            }
        }).start();
    }

    /**
     * catalog 监控逻辑
     * @param catalogConfigurationDir
     * @throws IOException
     * @throws InterruptedException
     */
    private void startCatalogWatcher(File catalogConfigurationDir) throws IOException, InterruptedException {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Paths.get(catalogConfigurationDir.getAbsolutePath()).register(
                watchService, StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_DELETE,StandardWatchEventKinds.ENTRY_MODIFY
        );
        while (true) {
            WatchKey key = watchService.take();
            log.info("Take watch enents");
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    log.info("New file in catalog directory :" + event.context());
                    Path newCatalog = (Path) event.context();
                    addCatalog(newCatalog);
                } else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    log.info("Delete file from catalog directory :" + event.context());
                    Path deletedCatalog = (Path) event.context();
                    deleteCatalog(deletedCatalog);
                } else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    log.info("Modify file from the catalog directory:" + event.context());
                    Path modifiedCatalog = (Path) event.context();
                    modifyCatalog(modifiedCatalog);
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }

    private void addCatalog(Path catalogPath) {
        log.info("addCatalog, catalogPath:" + catalogPath);
        File file = new File(catalogConfigurationDir, catalogPath.getFileName().toString());
        if (file.isFile() && file.getName().endsWith(".properties")) {
            try {
                TimeUnit.SECONDS.sleep(5);
                loadCatalog(file);
                PrestoServer.updateDataSourceAnnouncement(Files.getNameWithoutExtension(catalogPath.getFileName().toString()), PrestoServer.DatasourceAction.ADD);
            } catch (Exception e) {
                log.info("addCatalog error:" + e.getMessage());
            }
        }
    }

    private void deleteCatalog(Path catalogPath) {
        if (catalogPath.getFileName().toString().endsWith(".properties")) {
            String catalogName = Files.getNameWithoutExtension(catalogPath.getFileName().toString());
            log.info("-- Removing catalog %s", catalogName);
            connectorManager.removeConnector(catalogName);
            PrestoServer.updateDataSourceAnnouncement(catalogName, PrestoServer.DatasourceAction.DELETE);
        }
    }

    private void modifyCatalog(Path catalogPath) {
        deleteCatalog(catalogPath);
        addCatalog(catalogPath);
    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", file);
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String connectorName = properties.remove("connector.name");
        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static Map<String, String> loadProperties(File file)
            throws Exception
    {
        requireNonNull(file, "file is null");

        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
