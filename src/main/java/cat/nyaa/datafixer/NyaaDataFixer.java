package cat.nyaa.datafixer;

import cat.nyaa.nyaacore.configuration.NbtItemStack;
import cat.nyaa.nyaacore.utils.ItemStackUtils;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import com.mojang.datafixers.DataFixer;
import com.mojang.serialization.Dynamic;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.FloatTag;
import net.minecraft.nbt.ListTag;
import net.minecraft.nbt.NbtOps;
import net.minecraft.nbt.NumericTag;
import net.minecraft.nbt.Tag;
import net.minecraft.nbt.TagParser;
import net.minecraft.resources.RegistryOps;
import net.minecraft.util.datafix.DataFixers;
import net.minecraft.util.datafix.fixes.References;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.craftbukkit.CraftServer;
import org.bukkit.craftbukkit.inventory.CraftItemStack;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.TextComponent;
import net.kyori.adventure.text.format.TextDecoration;
import net.kyori.adventure.text.format.Style;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class NyaaDataFixer extends JavaPlugin {
    private static final int DEFAULT_ITEMSTACK_DATA_VERSION = 1139;
    private static final Pattern BASE64_PATTERN = Pattern.compile("^[A-Za-z0-9+/=]+$");

    private int currentDataVersion;
    private boolean logUpdatedItemNames;
    private int logUpdatedItemNamesRemaining;
    private String currentContext;
    private RegistryOps<Tag> registryOps;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        currentDataVersion = Bukkit.getUnsafe().getDataVersion();

        MigrationConfig config = MigrationConfig.from(this);
        logUpdatedItemNames = config.logUpdatedItemNames;
        logUpdatedItemNamesRemaining = config.logUpdatedItemNamesLimit;
        MigrationStats total = new MigrationStats();

        Path pluginsDir = getServer().getPluginsFolder().toPath();
        for (String target : config.targets) {
            Path targetDir = pluginsDir.resolve(target);
            if (!Files.exists(targetDir)) {
                getLogger().info("Skip missing target: " + targetDir);
                continue;
            }
            migrateTarget(targetDir, config, total);
        }

        getLogger().info(String.format(
                "Migration done. scanned=%d updated=%d base64=%d itemStacks=%d nbt=%d dbFiles=%d dbUpdates=%d dbCells=%d errors=%d",
                total.filesScanned, total.filesUpdated, total.base64Updated,
                total.itemStackUpdated, total.entityNbtUpdated,
                total.dbFilesScanned, total.dbFilesUpdated, total.dbCellsUpdated, total.errors));

        if (config.disableAfterRun) {
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    private void migrateTarget(Path targetDir, MigrationConfig config, MigrationStats total) {
        try (Stream<Path> stream = Files.walk(targetDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> isYamlFile(path.getFileName().toString()))
                    .filter(path -> !isExcluded(path, targetDir, config.excludeDirs))
                    .forEach(path -> migrateFile(path, targetDir, config, total));
        } catch (IOException ex) {
            total.errors++;
            getLogger().warning("Failed to scan " + targetDir + ": " + ex.getMessage());
        }
        migrateSqliteTargets(targetDir, config, total);
    }

    private void migrateFile(Path file, Path targetDir, MigrationConfig config, MigrationStats total) {
        total.filesScanned++;
        YamlConfiguration yaml = new YamlConfiguration();
        try {
            yaml.load(file.toFile());
        } catch (IOException | InvalidConfigurationException ex) {
            total.errors++;
            getLogger().warning("Failed to read " + file + ": " + ex.getMessage());
            return;
        }

        MigrationStats fileStats = new MigrationStats();
        String previousContext = currentContext;
        currentContext = formatContext(file, targetDir);
        boolean changed;
        try {
            changed = upgradeSection(yaml, fileStats);
        } finally {
            currentContext = previousContext;
        }
        if (!changed) {
            return;
        }

        total.merge(fileStats);
        total.filesUpdated++;

        if (config.dryRun) {
            getLogger().info("Would update " + file);
            return;
        }

        try {
            backupFile(file, targetDir, config);
            yaml.save(file.toFile());
        } catch (IOException ex) {
            total.errors++;
            getLogger().warning("Failed to write " + file + ": " + ex.getMessage());
        }
    }

    private void migrateSqliteTargets(Path targetDir, MigrationConfig config, MigrationStats total) {
        if (!config.sqliteEnabled || config.sqliteMatchers.isEmpty()) {
            return;
        }
        try (Stream<Path> stream = Files.walk(targetDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> matchesSqlitePattern(path.getFileName(), config.sqliteMatchers))
                    .forEach(path -> migrateSqliteFile(path, targetDir, config, total));
        } catch (IOException ex) {
            total.errors++;
            getLogger().warning("Failed to scan sqlite files in " + targetDir + ": " + ex.getMessage());
        }
    }

    private boolean matchesSqlitePattern(Path fileName, List<java.nio.file.PathMatcher> matchers) {
        for (var matcher : matchers) {
            if (matcher.matches(fileName)) {
                return true;
            }
        }
        return false;
    }

    private void migrateSqliteFile(Path dbFile, Path targetDir, MigrationConfig config, MigrationStats total) {
        total.dbFilesScanned++;
        boolean updated = false;
        try {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException ignored) {
                // Driver might be loaded via SPI.
            }
            try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath())) {
                connection.setAutoCommit(false);
                List<String> tables = listTables(connection);
                for (String table : tables) {
                    if (config.sqliteExcludeTables.contains(normalizeIdentifier(table))) {
                        continue;
                    }
                    TableSchema schema = readSchema(connection, table);
                    if (schema.textColumns.isEmpty()) {
                        continue;
                    }
                    for (String column : schema.textColumns) {
                        if (config.sqliteExcludeColumns.contains(normalizeIdentifier(column))) {
                            continue;
                        }
                        String previousContext = currentContext;
                        currentContext = formatContext(dbFile, targetDir) + "#" + table + "." + column;
                        try {
                            updated |= updateSqliteColumn(connection, table, column, schema, config, total);
                        } finally {
                            currentContext = previousContext;
                        }
                    }
                }
                if (updated && !config.dryRun) {
                    connection.commit();
                    total.dbFilesUpdated++;
                } else {
                    connection.rollback();
                }
            }
        } catch (SQLException ex) {
            total.errors++;
            getLogger().warning("Failed to migrate sqlite " + dbFile + ": " + ex.getMessage());
        }
        if (updated && config.dryRun) {
            getLogger().info("Would update sqlite " + dbFile);
        }
    }

    private boolean updateSqliteColumn(Connection connection, String table, String column, TableSchema schema,
                                       MigrationConfig config, MigrationStats total) throws SQLException {
        if (schema.hasRowId) {
            return updateSqliteColumnWithRowId(connection, table, column, config, total);
        }
        if (schema.primaryKeys.isEmpty()) {
            return false;
        }
        return updateSqliteColumnWithPrimaryKey(connection, table, column, schema.primaryKeys, config, total);
    }

    private boolean updateSqliteColumnWithRowId(Connection connection, String table, String column,
                                                MigrationConfig config, MigrationStats total) throws SQLException {
        String selectSql = "SELECT rowid, \"" + column + "\" FROM \"" + table + "\" WHERE \"" + column + "\" IS NOT NULL";
        String updateSql = "UPDATE \"" + table + "\" SET \"" + column + "\"=? WHERE rowid=?";
        boolean updated = false;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(selectSql);
             PreparedStatement update = connection.prepareStatement(updateSql)) {
            while (rs.next()) {
                long rowId = rs.getLong(1);
                String value = rs.getString(2);
                if (value == null) {
                    continue;
                }
                String upgraded = upgradeString(value, total);
                if (upgraded != null && !upgraded.equals(value)) {
                    update.setString(1, upgraded);
                    update.setLong(2, rowId);
                    update.executeUpdate();
                    updated = true;
                    total.dbCellsUpdated++;
                }
            }
        } catch (SQLException ex) {
            if (ex.getMessage() != null && ex.getMessage().toLowerCase(Locale.ROOT).contains("rowid")) {
                return false;
            }
            throw ex;
        }
        return updated;
    }

    private boolean updateSqliteColumnWithPrimaryKey(Connection connection, String table, String column,
                                                     List<String> primaryKeys, MigrationConfig config,
                                                     MigrationStats total) throws SQLException {
        StringBuilder selectSql = new StringBuilder("SELECT ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) selectSql.append(", ");
            selectSql.append('"').append(primaryKeys.get(i)).append('"');
        }
        selectSql.append(", \"").append(column).append("\" FROM \"").append(table).append("\" WHERE \"")
                .append(column).append("\" IS NOT NULL");

        StringBuilder updateSql = new StringBuilder("UPDATE \"").append(table).append("\" SET \"")
                .append(column).append("\"=? WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) updateSql.append(" AND ");
            updateSql.append('"').append(primaryKeys.get(i)).append("\"=?");
        }

        boolean updated = false;
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(selectSql.toString());
             PreparedStatement update = connection.prepareStatement(updateSql.toString())) {
            while (rs.next()) {
                int index = 1;
                Object[] pkValues = new Object[primaryKeys.size()];
                for (int i = 0; i < primaryKeys.size(); i++) {
                    pkValues[i] = rs.getObject(index++);
                }
                String value = rs.getString(index);
                if (value == null) {
                    continue;
                }
                String upgraded = upgradeString(value, total);
                if (upgraded != null && !upgraded.equals(value)) {
                    update.setString(1, upgraded);
                    for (int i = 0; i < pkValues.length; i++) {
                        update.setObject(2 + i, pkValues[i]);
                    }
                    update.executeUpdate();
                    updated = true;
                    total.dbCellsUpdated++;
                }
            }
        }
        return updated;
    }

    private List<String> listTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    private TableSchema readSchema(Connection connection, String table) throws SQLException {
        List<String> textColumns = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        boolean hasRowId = true;
        String pragma = "PRAGMA table_info(\"" + table + "\")";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(pragma)) {
            while (rs.next()) {
                String column = rs.getString("name");
                String type = rs.getString("type");
                int pk = rs.getInt("pk");
                if (pk > 0) {
                    primaryKeys.add(column);
                }
                if (type != null) {
                    String upper = type.toUpperCase(Locale.ROOT);
                    if (upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB")) {
                        textColumns.add(column);
                    }
                }
            }
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.executeQuery("SELECT rowid FROM \"" + table + "\" LIMIT 1");
        } catch (SQLException ex) {
            hasRowId = false;
        }
        return new TableSchema(textColumns, primaryKeys, hasRowId);
    }

    private boolean upgradeSection(ConfigurationSection section, MigrationStats stats) {
        boolean changed = false;
        for (String key : section.getKeys(false)) {
            Object value = section.get(key);
            if (value instanceof ConfigurationSection nested) {
                if (upgradeSection(nested, stats)) {
                    changed = true;
                }
                continue;
            }
            Object updated = upgradeObject(value, stats);
            if (updated != value && !Objects.equals(updated, value)) {
                section.set(key, updated);
                changed = true;
            }
        }
        return changed;
    }

    private Object upgradeObject(Object value, MigrationStats stats) {
        if (value instanceof String text) {
            return upgradeString(text, stats);
        }
        if (value instanceof ItemStack item) {
            return upgradeItemStack(item, stats);
        }
        if (value instanceof NbtItemStack nbtItem) {
            if (nbtItem.it == null) {
                return nbtItem;
            }
            ItemStack upgraded = upgradeItemStack(nbtItem.it, stats);
            // Force reserialization so base64 gets rewritten with the new format.
            return new NbtItemStack(upgraded);
        }
        if (value instanceof Map<?, ?> map) {
            return upgradeMap(map, stats);
        }
        if (value instanceof List<?> list) {
            return upgradeList(list, stats);
        }
        return value;
    }

    private Object upgradeMap(Map<?, ?> map, MigrationStats stats) {
        boolean changed = false;
        Map<Object, Object> updated = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Object newValue = upgradeObject(value, stats);
            if (newValue != value && !Objects.equals(newValue, value)) {
                changed = true;
            }
            updated.put(key, newValue);
        }
        return changed ? updated : map;
    }

    private Object upgradeList(List<?> list, MigrationStats stats) {
        boolean changed = false;
        List<Object> updated = new ArrayList<>(list.size());
        for (Object entry : list) {
            Object newValue = upgradeObject(entry, stats);
            if (newValue != entry && !Objects.equals(newValue, entry)) {
                changed = true;
            }
            updated.add(newValue);
        }
        return changed ? updated : list;
    }

    private String upgradeString(String text, MigrationStats stats) {
        if (text == null || text.isEmpty() || "<null>".equalsIgnoreCase(text)) {
            return text;
        }
        String trimmed = text.trim();
        String normalized = normalizeJsonComponentText(trimmed);
        if (normalized != null) {
            return normalized;
        }
        if (looksLikeNbt(trimmed)) {
            String updated = upgradeEntityNbt(trimmed, stats);
            return updated == null ? text : updated;
        }
        String updated = upgradeBase64Item(trimmed, stats);
        return updated == null ? text : updated;
    }

    private ItemStack upgradeItemStack(ItemStack item, MigrationStats stats) {
        if (item == null || item.isEmpty()) {
            return item;
        }
        AtomicBoolean changed = new AtomicBoolean(false);
        ItemStack upgraded = upgradeItemStackInternal(item, changed);
        if (!changed.get()) {
            return item;
        }
        stats.itemStackUpdated++;
        return upgraded;
    }

    private String upgradeBase64Item(String base64, MigrationStats stats) {
        if (base64.length() < 16 || !BASE64_PATTERN.matcher(base64).matches()) {
            return null;
        }
        try {
            ItemStack item = ItemStackUtils.itemFromBase64(base64);
            if (item == null) {
                return null;
            }
            AtomicBoolean changed = new AtomicBoolean(false);
            ItemStack upgraded = upgradeItemStackInternal(item, changed);
            if (changed.get()) {
                item = upgraded;
            }
            String updated = ItemStackUtils.itemToBase64(item);
            if (!updated.equals(base64) || changed.get()) {
                stats.base64Updated++;
                if (!changed.get()) {
                    logUpdatedItemName(item);
                }
                return updated;
            }
        } catch (Exception ignored) {
            return null;
        }
        return null;
    }

    private String upgradeEntityNbt(String nbt, MigrationStats stats) {
        try {
            CompoundTag tag = TagParser.parseCompoundFully(nbt);
            AtomicBoolean changed = new AtomicBoolean(false);
            Tag upgraded = upgradeItemTags(tag, changed);
            if (!changed.get() || !(upgraded instanceof CompoundTag)) {
                return null;
            }
            stats.entityNbtUpdated++;
            return upgraded.toString();
        } catch (CommandSyntaxException | RuntimeException ex) {
            return null;
        }
    }

    private Tag upgradeItemTags(Tag tag, AtomicBoolean changed) {
        if (tag instanceof CompoundTag compound) {
            if (isItemStackCompound(compound)) {
                return upgradeItemStackTag(compound, changed);
            }
            for (String key : compound.keySet()) {
                Tag child = compound.get(key);
                if (child == null) {
                    continue;
                }
                Tag upgradedChild = upgradeItemTags(child, changed);
                if (upgradedChild != child && !Objects.equals(upgradedChild, child)) {
                    compound.put(key, upgradedChild);
                }
            }
            if (normalizeEntityEquipment(compound)) {
                changed.set(true);
            }
            return compound;
        }
        if (tag instanceof ListTag list) {
            for (int i = 0; i < list.size(); i++) {
                Tag child = list.get(i);
                Tag upgradedChild = upgradeItemTags(child, changed);
                if (upgradedChild != child && !Objects.equals(upgradedChild, child)) {
                    list.set(i, upgradedChild);
                }
            }
            return list;
        }
        return tag;
    }

    private CompoundTag upgradeItemStackTag(CompoundTag tag, AtomicBoolean changed) {
        int dataVersion = tag.getInt("DataVersion").orElse(DEFAULT_ITEMSTACK_DATA_VERSION);
        boolean hadDataVersion = tag.get("DataVersion") != null;
        if (hadDataVersion && dataVersion >= currentDataVersion) {
            return tag;
        }
        boolean hasComponents = tag.get("components") instanceof CompoundTag;
        boolean hasLegacyTag = tag.get("tag") != null;
        boolean itemChanged = false;
        // Avoid running DataFixer on already-modern component-based stacks without a DataVersion,
        // otherwise it can clobber custom_model_data and other component values.
        if (!hadDataVersion && hasComponents && !hasLegacyTag) {
            itemChanged = normalizeCustomModelData(tag);
            CompoundTag normalized = normalizeItemText(tag, hadDataVersion);
            if (normalized != null) {
                tag = normalized;
                itemChanged = true;
            }
            if (upgradeNestedItemTags(tag, changed)) {
                itemChanged = true;
            }
            if (itemChanged) {
                changed.set(true);
                logUpdatedItemName(tag);
            }
            return tag;
        }
        DataFixer dataFixer = DataFixers.getDataFixer();
        Dynamic<Tag> dynamic = new Dynamic<>(NbtOps.INSTANCE, tag);
        dynamic = dataFixer.update(References.ITEM_STACK, dynamic, dataVersion, currentDataVersion);
        CompoundTag updated = (CompoundTag) dynamic.getValue();
        if (hadDataVersion) {
            updated.putInt("DataVersion", currentDataVersion);
        } else if (updated.get("DataVersion") != null) {
            updated.remove("DataVersion");
        }
        if (!updated.equals(tag)) {
            itemChanged = true;
        }
        if (normalizeCustomModelData(updated)) {
            itemChanged = true;
        }
        CompoundTag normalized = normalizeItemText(updated, hadDataVersion);
        if (normalized != null) {
            updated = normalized;
            itemChanged = true;
        }
        if (upgradeNestedItemTags(updated, changed)) {
            itemChanged = true;
        }
        if (itemChanged) {
            changed.set(true);
            logUpdatedItemName(updated);
        }
        return updated;
    }

    private ItemStack upgradeItemStackInternal(ItemStack item, AtomicBoolean changed) {
        CompoundTag tag = itemStackToTag(item);
        if (tag != null) {
            AtomicBoolean localChanged = new AtomicBoolean(false);
            CompoundTag updated = upgradeItemStackTag(tag, localChanged);
            if (localChanged.get()) {
                ItemStack upgraded = itemStackFromTag(updated);
                if (upgraded != null) {
                    changed.set(true);
                    return upgraded;
                }
            }
            return item;
        }
        try {
            byte[] raw = ItemStackUtils.itemToBinary(item);
            ItemStack upgraded = ItemStackUtils.itemFromBinary(raw);
            if (upgraded == null) {
                return item;
            }
            byte[] upgradedRaw = ItemStackUtils.itemToBinary(upgraded);
            boolean updated = !Arrays.equals(raw, upgradedRaw);
            if (normalizeItemText(upgraded)) {
                updated = true;
            }
            if (updated) {
                changed.set(true);
                logUpdatedItemName(upgraded);
                return upgraded;
            }
        } catch (Exception ignored) {
            // Leave item as-is if it cannot be re-encoded.
        }
        return item;
    }

    private boolean isItemStackCompound(CompoundTag tag) {
        return tag.get("id") != null && (tag.get("Count") != null || tag.get("count") != null);
    }

    private boolean normalizeCustomModelData(CompoundTag tag) {
        Tag componentsTag = tag.get("components");
        if (!(componentsTag instanceof CompoundTag components)) {
            return false;
        }
        Tag cmdTag = components.get("minecraft:custom_model_data");
        if (cmdTag == null) {
            return false;
        }
        if (cmdTag instanceof CompoundTag compound) {
            Tag floats = compound.get("floats");
            if (floats instanceof ListTag) {
                return false;
            }
        }
        Float value = readNumericTag(cmdTag);
        if (value == null) {
            return false;
        }
        ListTag floats = new ListTag();
        floats.add(FloatTag.valueOf(value));
        CompoundTag normalized = new CompoundTag();
        normalized.put("floats", floats);
        components.put("minecraft:custom_model_data", normalized);
        return true;
    }

    private Float readNumericTag(Tag tag) {
        if (tag instanceof NumericTag numericTag) {
            return numericTag.floatValue();
        }
        return null;
    }

    private boolean normalizeEntityEquipment(CompoundTag tag) {
        boolean updated = false;
        CompoundTag equipment = tag.get("equipment") instanceof CompoundTag existing ? existing : new CompoundTag();
        Tag armorTag = tag.get("ArmorItems");
        if (armorTag instanceof ListTag armorItems) {
            updated |= putEquipmentSlot(equipment, "feet", armorItems, 0);
            updated |= putEquipmentSlot(equipment, "legs", armorItems, 1);
            updated |= putEquipmentSlot(equipment, "chest", armorItems, 2);
            updated |= putEquipmentSlot(equipment, "head", armorItems, 3);
            tag.remove("ArmorItems");
            updated = true;
        }
        Tag handTag = tag.get("HandItems");
        if (handTag instanceof ListTag handItems) {
            updated |= putEquipmentSlot(equipment, "mainhand", handItems, 0);
            updated |= putEquipmentSlot(equipment, "offhand", handItems, 1);
            tag.remove("HandItems");
            updated = true;
        }
        if (updated && !equipment.isEmpty()) {
            tag.put("equipment", equipment);
        }
        return updated;
    }

    private boolean putEquipmentSlot(CompoundTag equipment, String key, ListTag list, int index) {
        if (equipment.contains(key)) {
            return false;
        }
        if (index >= list.size()) {
            return false;
        }
        Tag entry = list.get(index);
        if (entry instanceof CompoundTag compound && !compound.isEmpty()) {
            equipment.put(key, compound);
            return true;
        }
        return false;
    }

    private boolean looksLikeNbt(String text) {
        return text.startsWith("{") && text.endsWith("}") && text.length() > 2;
    }

    private boolean isYamlFile(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.endsWith(".yml") || lower.endsWith(".yaml");
    }

    private boolean isExcluded(Path file, Path root, Set<String> excludes) {
        Path relative = root.relativize(file);
        for (Path part : relative) {
            if (excludes.contains(part.toString().toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private void backupFile(Path file, Path targetRoot, MigrationConfig config) throws IOException {
        if (!config.backupEnabled) {
            return;
        }
        if (config.backupDirectory == null || config.backupDirectory.isEmpty()) {
            Path backup = file.resolveSibling(file.getFileName().toString() + config.backupSuffix);
            Files.copy(file, backup, StandardCopyOption.REPLACE_EXISTING);
            return;
        }
        Path backupRoot = getDataFolder().toPath().resolve(config.backupDirectory);
        Path relative = targetRoot.relativize(file);
        Path backup = backupRoot.resolve(targetRoot.getFileName().toString()).resolve(relative);
        Files.createDirectories(backup.getParent());
        if (config.backupSuffix != null && !config.backupSuffix.isEmpty()) {
            backup = backup.resolveSibling(backup.getFileName().toString() + config.backupSuffix);
        }
        Files.copy(file, backup, StandardCopyOption.REPLACE_EXISTING);
    }

    private void logUpdatedItemName(ItemStack item) {
        if (!logUpdatedItemNames || logUpdatedItemNamesRemaining <= 0) {
            return;
        }
        String name = extractCustomItemName(item);
        if (name == null || name.isBlank()) {
            return;
        }
        StringBuilder message = new StringBuilder("Updated item name");
        if (currentContext != null) {
            message.append(" in ").append(currentContext);
        }
        message.append(": ").append(sanitizeLogValue(name));
        getLogger().info(message.toString());
        logUpdatedItemNamesRemaining--;
    }

    private void logUpdatedItemName(CompoundTag tag) {
        if (!logUpdatedItemNames || logUpdatedItemNamesRemaining <= 0) {
            return;
        }
        ItemStack item = itemStackFromTag(tag);
        if (item != null) {
            logUpdatedItemName(item);
        }
    }

    private ItemStack itemStackFromTag(CompoundTag tag) {
        try {
            RegistryOps<Tag> ops = getRegistryOps();
            var result = net.minecraft.world.item.ItemStack.CODEC.parse(ops, tag);
            var nms = result.result().orElse(null);
            if (nms == null) {
                return null;
            }
            return CraftItemStack.asBukkitCopy(nms);
        } catch (Exception ignored) {
            return null;
        }
    }

    private CompoundTag itemStackToTag(ItemStack item) {
        try {
            RegistryOps<Tag> ops = getRegistryOps();
            var nms = CraftItemStack.asNMSCopy(item);
            var result = net.minecraft.world.item.ItemStack.CODEC.encodeStart(ops, nms);
            Tag value = result.result().orElse(null);
            if (value instanceof CompoundTag compound) {
                return compound;
            }
        } catch (Exception ignored) {
            // Ignore serialization errors.
        }
        return null;
    }

    private RegistryOps<Tag> getRegistryOps() {
        if (registryOps == null) {
            CraftServer server = (CraftServer) Bukkit.getServer();
            registryOps = RegistryOps.create(NbtOps.INSTANCE, server.getServer().registryAccess());
        }
        return registryOps;
    }

    private String extractCustomItemName(ItemStack item) {
        if (item == null || item.isEmpty()) {
            return null;
        }
        if (!item.hasItemMeta()) {
            return null;
        }
        ItemMeta meta = item.getItemMeta();
        if (meta == null) {
            return null;
        }
        Component component = null;
        if (meta.hasCustomName()) {
            component = meta.customName();
        } else if (meta.hasDisplayName()) {
            component = meta.displayName();
        }
        if (component != null) {
            String plain = PlainTextComponentSerializer.plainText().serialize(component);
            if (plain != null && !plain.isBlank()) {
                return plain;
            }
        }
        if (meta.hasDisplayName()) {
            String legacy = meta.getDisplayName();
            if (legacy != null && !legacy.isBlank()) {
                return legacy;
            }
        }
        return null;
    }

    private boolean normalizeItemText(ItemStack item) {
        if (item == null || item.isEmpty() || !item.hasItemMeta()) {
            return false;
        }
        ItemMeta meta = item.getItemMeta();
        if (meta == null) {
            return false;
        }
        boolean changed = false;
        boolean hasCustomName = meta.hasCustomName();
        if (hasCustomName) {
            Component current = meta.customName();
            if (current != null) {
                Component normalized = normalizeComponent(current);
                if (normalized != null && !normalized.equals(current)) {
                    meta.customName(normalized);
                    current = normalized;
                    changed = true;
                }
                String plain = PlainTextComponentSerializer.plainText().serialize(current);
                Component parsed = parseTextComponent(plain);
                if (parsed != null && !parsed.equals(current)) {
                    meta.customName(parsed);
                    changed = true;
                }
            }
        }
        if (!hasCustomName && meta.hasDisplayName()) {
            String legacy = meta.getDisplayName();
            Component parsed = parseTextComponent(legacy);
            if (parsed != null) {
                Component current = meta.displayName();
                if (current == null || !current.equals(parsed)) {
                    meta.displayName(parsed);
                    changed = true;
                }
            } else {
                Component current = meta.displayName();
                if (current != null) {
                    Component normalized = normalizeComponent(current);
                    if (normalized != null && !normalized.equals(current)) {
                        meta.displayName(normalized);
                        changed = true;
                    }
                }
            }
        }
        List<Component> loreComponents = meta.lore();
        if (loreComponents != null && !loreComponents.isEmpty()) {
            boolean needsNormalize = false;
            List<Component> normalized = new ArrayList<>(loreComponents.size());
            for (Component line : loreComponents) {
                String plain = PlainTextComponentSerializer.plainText().serialize(line);
                Component parsed = parseTextComponent(plain);
                Component target = parsed != null ? parsed : line;
                Component compacted = normalizeComponent(target);
                if (compacted != null && !compacted.equals(line)) {
                    needsNormalize = true;
                }
                normalized.add(compacted != null ? compacted : line);
            }
            if (needsNormalize) {
                meta.lore(normalized);
                changed = true;
            }
        } else if (meta.hasLore()) {
            List<String> lore = meta.getLore();
            if (lore != null && !lore.isEmpty()) {
                boolean needsNormalize = false;
                for (String line : lore) {
                    if (looksLikeJsonComponent(line) || containsLegacyCodes(line)) {
                        needsNormalize = true;
                        break;
                    }
                }
                if (needsNormalize) {
                    List<Component> normalized = new ArrayList<>(lore.size());
                    for (String line : lore) {
                        Component parsed = parseTextComponent(line);
                        Component fallback = parsed != null ? parsed : Component.text(line);
                        normalized.add(normalizeComponent(fallback));
                    }
                    meta.lore(normalized);
                    changed = true;
                }
            }
        }
        if (changed) {
            item.setItemMeta(meta);
        }
        return changed;
    }

    private CompoundTag normalizeItemText(CompoundTag tag, boolean hadDataVersion) {
        ItemStack item = itemStackFromTag(tag);
        if (item == null) {
            return null;
        }
        if (!normalizeItemText(item)) {
            return null;
        }
        CompoundTag normalized = itemStackToTag(item);
        if (normalized == null) {
            return null;
        }
        if (hadDataVersion) {
            normalized.putInt("DataVersion", currentDataVersion);
        } else if (normalized.get("DataVersion") != null) {
            normalized.remove("DataVersion");
        }
        return normalized;
    }

    private boolean upgradeNestedItemTags(CompoundTag tag, AtomicBoolean changed) {
        CompoundTag before = tag.copy();
        upgradeNestedItemTagsInternal(tag, changed);
        return !before.equals(tag);
    }

    private void upgradeNestedItemTagsInternal(Tag tag, AtomicBoolean changed) {
        if (tag instanceof CompoundTag compound) {
            for (String key : compound.keySet()) {
                Tag child = compound.get(key);
                if (child == null) {
                    continue;
                }
                if (child instanceof CompoundTag childCompound) {
                    if (isItemStackCompound(childCompound)) {
                        Tag upgradedChild = upgradeItemStackTag(childCompound, changed);
                        if (upgradedChild != childCompound && !Objects.equals(upgradedChild, childCompound)) {
                            compound.put(key, upgradedChild);
                        }
                    } else {
                        upgradeNestedItemTagsInternal(childCompound, changed);
                    }
                    continue;
                }
                if (child instanceof ListTag list) {
                    upgradeNestedItemTagsInternal(list, changed);
                }
            }
            return;
        }
        if (tag instanceof ListTag list) {
            for (int i = 0; i < list.size(); i++) {
                Tag child = list.get(i);
                if (child instanceof CompoundTag childCompound) {
                    if (isItemStackCompound(childCompound)) {
                        Tag upgradedChild = upgradeItemStackTag(childCompound, changed);
                        if (upgradedChild != childCompound && !Objects.equals(upgradedChild, childCompound)) {
                            list.set(i, upgradedChild);
                        }
                    } else {
                        upgradeNestedItemTagsInternal(childCompound, changed);
                    }
                    continue;
                }
                if (child instanceof ListTag childList) {
                    upgradeNestedItemTagsInternal(childList, changed);
                }
            }
        }
    }

    private Component parseTextComponent(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = stripWrappedQuotes(raw.trim());
        if (looksLikeJsonComponent(trimmed)) {
            try {
                Component parsed = GsonComponentSerializer.gson().deserialize(trimmed);
                String plain = PlainTextComponentSerializer.plainText().serialize(parsed);
                if (containsLegacyCodes(plain)) {
                    return normalizeComponent(LegacyComponentSerializer.legacySection().deserialize(plain));
                }
                return normalizeComponent(parsed);
            } catch (Exception ignored) {
                // Fall back to legacy parsing below.
            }
        }
        if (containsLegacyCodes(trimmed)) {
            return normalizeComponent(LegacyComponentSerializer.legacySection().deserialize(trimmed));
        }
        return null;
    }

    private Component normalizeComponent(Component component) {
        if (component == null) {
            return null;
        }
        return ensureExtraWrapper(component);
    }

    private Component ensureExtraWrapper(Component component) {
        List<Component> extras = new ArrayList<>();
        flattenForExtra(component, Style.empty(), extras);
        if (extras.isEmpty()) {
            return Component.text("");
        }
        return Component.text("").append(extras);
    }

    private void flattenForExtra(Component component, Style parentStyle, List<Component> extras) {
        if (component == null) {
            return;
        }
        Style mergedStyle = component.style();
        if (parentStyle != null) {
            mergedStyle = mergedStyle.merge(parentStyle, Style.Merge.Strategy.IF_ABSENT_ON_TARGET);
        }
        if (component instanceof TextComponent text) {
            if (!text.content().isEmpty()) {
                Component leaf = Component.text(text.content()).style(mergedStyle);
                extras.add(normalizeDecorationsIfUnset(leaf));
            }
        } else if (component.children().isEmpty()) {
            Component leaf = component.style(mergedStyle).children(Collections.emptyList());
            extras.add(normalizeDecorationsIfUnset(leaf));
        }
        if (!component.children().isEmpty()) {
            for (Component child : component.children()) {
                flattenForExtra(child, mergedStyle, extras);
            }
        }
    }

    private Component normalizeDecorationsIfUnset(Component component) {
        if (component == null) {
            return null;
        }
        Component normalized = component;
        for (TextDecoration decoration : TextDecoration.values()) {
            if (normalized.decoration(decoration) == TextDecoration.State.NOT_SET) {
                normalized = normalized.decoration(decoration, TextDecoration.State.FALSE);
            }
        }
        return normalized;
    }

    private String stripWrappedQuotes(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        char first = value.charAt(0);
        char last = value.charAt(value.length() - 1);
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private boolean looksLikeJsonComponent(String text) {
        if (text == null) {
            return false;
        }
        String trimmed = text.trim();
        if (!(trimmed.startsWith("{\"") || trimmed.startsWith("["))) {
            return false;
        }
        if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return trimmed.contains("\"text\"")
                    || trimmed.contains("\"extra\"")
                    || trimmed.contains("\"translate\"")
                    || trimmed.contains("\"color\"")
                    || trimmed.contains("\"bold\"")
                    || trimmed.contains("\"italic\"")
                    || trimmed.contains("\"underlined\"")
                    || trimmed.contains("\"strikethrough\"")
                    || trimmed.contains("\"obfuscated\"");
        }
        return false;
    }

    private String normalizeJsonComponentText(String value) {
        if (!looksLikeJsonComponent(value)) {
            return null;
        }
        try {
            Component parsed = GsonComponentSerializer.gson().deserialize(value);
            return PlainTextComponentSerializer.plainText().serialize(parsed);
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean containsLegacyCodes(String text) {
        return text != null && text.indexOf('§') >= 0;
    }

    private String sanitizeLogValue(String value) {
        return value.replace("\n", "\\n").replace("\r", "\\r");
    }

    private String formatContext(Path file, Path targetDir) {
        Path relative = targetDir.relativize(file);
        return targetDir.getFileName() + "/" + relative.toString().replace(File.separatorChar, '/');
    }

    private static class MigrationConfig {
        final List<String> targets;
        final Set<String> excludeDirs;
        final boolean backupEnabled;
        final String backupDirectory;
        final String backupSuffix;
        final boolean dryRun;
        final boolean disableAfterRun;
        final boolean sqliteEnabled;
        final List<java.nio.file.PathMatcher> sqliteMatchers;
        final Set<String> sqliteExcludeTables;
        final Set<String> sqliteExcludeColumns;
        final boolean logUpdatedItemNames;
        final int logUpdatedItemNamesLimit;

        private MigrationConfig(List<String> targets, Set<String> excludeDirs, boolean backupEnabled,
                                String backupDirectory, String backupSuffix, boolean dryRun, boolean disableAfterRun,
                                boolean sqliteEnabled, List<java.nio.file.PathMatcher> sqliteMatchers,
                                Set<String> sqliteExcludeTables, Set<String> sqliteExcludeColumns,
                                boolean logUpdatedItemNames, int logUpdatedItemNamesLimit) {
            this.targets = targets;
            this.excludeDirs = excludeDirs;
            this.backupEnabled = backupEnabled;
            this.backupDirectory = backupDirectory;
            this.backupSuffix = backupSuffix == null ? "" : backupSuffix;
            this.dryRun = dryRun;
            this.disableAfterRun = disableAfterRun;
            this.sqliteEnabled = sqliteEnabled;
            this.sqliteMatchers = sqliteMatchers;
            this.sqliteExcludeTables = sqliteExcludeTables;
            this.sqliteExcludeColumns = sqliteExcludeColumns;
            this.logUpdatedItemNames = logUpdatedItemNames;
            this.logUpdatedItemNamesLimit = Math.max(0, logUpdatedItemNamesLimit);
        }

        static MigrationConfig from(JavaPlugin plugin) {
            List<String> targets = plugin.getConfig().getStringList("targets");
            if (targets == null || targets.isEmpty()) {
                targets = Collections.emptyList();
            }
            List<String> excludes = plugin.getConfig().getStringList("excludeDirs");
            Set<String> excludeDirs = new HashSet<>();
            for (String entry : excludes) {
                if (entry != null && !entry.isEmpty()) {
                    excludeDirs.add(entry.toLowerCase(Locale.ROOT));
                }
            }
            ConfigurationSection backup = plugin.getConfig().getConfigurationSection("backup");
            boolean backupEnabled = backup == null || backup.getBoolean("enabled", true);
            String backupDirectory = backup == null ? "backup" : backup.getString("directory", "backup");
            String backupSuffix = backup == null ? ".bak" : backup.getString("suffix", ".bak");
            boolean dryRun = plugin.getConfig().getBoolean("dryRun", false);
            boolean disableAfterRun = plugin.getConfig().getBoolean("disableAfterRun", true);
            ConfigurationSection sqlite = plugin.getConfig().getConfigurationSection("sqlite");
            boolean sqliteEnabled = sqlite == null || sqlite.getBoolean("enabled", true);
            List<String> includePatterns = sqlite == null ? Collections.singletonList("*.db") : sqlite.getStringList("includePatterns");
            if (includePatterns == null || includePatterns.isEmpty()) {
                includePatterns = Collections.singletonList("*.db");
            }
            List<java.nio.file.PathMatcher> matchers = new ArrayList<>();
            for (String pattern : includePatterns) {
                if (pattern != null && !pattern.isBlank()) {
                    matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
                }
            }
            Set<String> excludeTables = new HashSet<>();
            Set<String> excludeColumns = new HashSet<>();
            if (sqlite != null) {
                for (String entry : sqlite.getStringList("excludeTables")) {
                    if (entry != null && !entry.isBlank()) {
                        excludeTables.add(normalizeIdentifier(entry));
                    }
                }
                for (String entry : sqlite.getStringList("excludeColumns")) {
                    if (entry != null && !entry.isBlank()) {
                        excludeColumns.add(normalizeIdentifier(entry));
                    }
                }
            }
            boolean logUpdatedItemNames = plugin.getConfig().getBoolean("logUpdatedItemNames", false);
            int logUpdatedItemNamesLimit = plugin.getConfig().getInt("logUpdatedItemNamesLimit", 200);
            return new MigrationConfig(targets, excludeDirs, backupEnabled, backupDirectory, backupSuffix,
                    dryRun, disableAfterRun, sqliteEnabled, matchers, excludeTables, excludeColumns,
                    logUpdatedItemNames, logUpdatedItemNamesLimit);
        }
    }

    private static class MigrationStats {
        int filesScanned;
        int filesUpdated;
        int base64Updated;
        int itemStackUpdated;
        int entityNbtUpdated;
        int dbFilesScanned;
        int dbFilesUpdated;
        int dbCellsUpdated;
        int errors;

        void merge(MigrationStats other) {
            base64Updated += other.base64Updated;
            itemStackUpdated += other.itemStackUpdated;
            entityNbtUpdated += other.entityNbtUpdated;
            dbFilesScanned += other.dbFilesScanned;
            dbFilesUpdated += other.dbFilesUpdated;
            dbCellsUpdated += other.dbCellsUpdated;
            errors += other.errors;
        }
    }

    private static class TableSchema {
        final List<String> textColumns;
        final List<String> primaryKeys;
        final boolean hasRowId;

        private TableSchema(List<String> textColumns, List<String> primaryKeys, boolean hasRowId) {
            this.textColumns = textColumns;
            this.primaryKeys = primaryKeys;
            this.hasRowId = hasRowId;
        }
    }

    private static String normalizeIdentifier(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
