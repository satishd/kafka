/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * This tool is used to alter configurations for multiple entities at once. The tool reads a list of entity names from
 * a file and applies the specified configuration changes to each entity. It is similar to the `kafka-configs.sh` tool.
 */
public class AlterConfigBatchCommand {
    private static final int DEFAULT_TIMEOUT_MINUTES = 4;

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);
            String entityNamesFile = res.getString("entityNamesFile");
            String bootstrapServer = res.getString("bootstrapServer");
            String entityType = res.getString("entityType");
            String addConfig = res.getString("addConfig");
            String deleteConfig = res.getString("deleteConfig");
            String commandConfig = res.getString("commandConfig");

            List<AlterConfigOp> configOps = new ArrayList<>();
            if (addConfig != null) {
                configOps.addAll(getAddConfigOps(addConfig));
            } else if (deleteConfig != null) {
                configOps.addAll(getDeleteConfigOps(deleteConfig));
            }
            Set<String> entityNames = parseEntityNames(entityNamesFile);

            ConfigResource.Type type;
            switch (entityType) {
                case "topics":
                    type = ConfigResource.Type.TOPIC;
                    break;
                case "brokers":
                    type = ConfigResource.Type.BROKER;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid entity type: " + entityType + ". Valid values are 'topics' and 'brokers'.");
            }

            Map<ConfigResource, Collection<AlterConfigOp>> configOperations = new HashMap<>();
            for (String entityName : entityNames) {
                ConfigResource resource = new ConfigResource(type, entityName);
                configOperations.put(resource, configOps);
            }

            Properties props = loadProperties(commandConfig, bootstrapServer);
            try (Admin admin = Admin.create(props)) {
                applyAlterConfig(admin, configOperations);
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        } finally {
            Exit.exit(0);
        }
    }

    private static Properties loadProperties(String commandConfig, String bootstrapServer) {
        Properties props;
        try {
            props = Utils.loadProps(commandConfig);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error loading properties from command config file: " + commandConfig, e);
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return props;
    }

    private static Set<String> parseEntityNames(String entityNamesFile) throws Exception {
        List<String> entityNames;
        entityNames = Files.readAllLines(Paths.get(entityNamesFile), StandardCharsets.UTF_8);
        // ensuring no repeated entity names
        Set<String> entityNamesSet = new HashSet<>(entityNames);
        if (entityNamesSet.size() != entityNames.size()) {
            System.out.println("[WARN] The entity names in the file contain duplicate entries.");
        }
        return entityNamesSet;
    }

    private static void applyAlterConfig(Admin admin, Map<ConfigResource, Collection<AlterConfigOp>> configOperations) {
        AlterConfigsResult alterConfigsResult = admin.incrementalAlterConfigs(configOperations);
        Map<ConfigResource, KafkaFuture<Void>> futures = alterConfigsResult.values();
        for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : futures.entrySet()) {
            ConfigResource resource = entry.getKey();
            KafkaFuture<Void> future = entry.getValue();
            try {
                future.get(DEFAULT_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                System.out.println("SUCCESS updating config for: " + resource.name());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("FAILURE updating config for: " + resource.name());
            }
        }
    }

    private static List<AlterConfigOp> getDeleteConfigOps(String deleteConfig) {
        List<AlterConfigOp> configOps = new ArrayList<>();
        for (String config : deleteConfig.split(",")) {
            ConfigEntry configEntry = new ConfigEntry(config, null);
            AlterConfigOp op = new AlterConfigOp(configEntry, AlterConfigOp.OpType.DELETE);
            configOps.add(op);
        }
        return configOps;
    }

    private static List<AlterConfigOp> getAddConfigOps(String addConfig) {
        List<AlterConfigOp> configOps = new ArrayList<>();
        for (String config : addConfig.split(",")) {
            String[] keyValue = config.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid config: " + config);
            }
            ConfigEntry configEntry = new ConfigEntry(keyValue[0], keyValue[1]);
            AlterConfigOp op = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
            configOps.add(op);
        }
        return configOps;
    }

    /**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-configs-batch-alter")
                .defaultHelp(true)
                .description("This tool is used to alter configurations for multiple entities at once.\n" +
                        "Example usage:\n" +
                        "bin/kafka-configs-batch-alter.sh --entity-names-file /tmp/entity-names.txt --bootstrap-server localhost:9092 --entity-type topics --add-config k1=v1,k2=v2\n" +
                        "bin/kafka-configs-batch-alter.sh --entity-names-file /tmp/entity-names.txt --bootstrap-server localhost:9092 --entity-type brokers --delete-config k1,k2" +
                        "Configs with values as lists are not supported. Use kafka-configs.sh for those updates");

        parser.addArgument("--entity-names-file")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ENTITY-NAMES-FILE")
                .dest("entityNamesFile")
                .help("file containing the list of entities to alter.");

        parser.addArgument("--bootstrap-server")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("BOOTSTRAP-SERVER")
                .dest("bootstrapServer")
                .help("The Kafka server to connect to.");

        parser.addArgument("--entity-type")
                .action(store())
                .required(true)
                .type(String.class)
                .choices("topics", "brokers")
                .metavar("ENTITY-TYPE")
                .dest("entityType")
                .help("The type of entity to alter.");

        parser.addArgument("--command-config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("COMMAND-CONFIG")
                .dest("commandConfig")
                .help("command config property file");

        MutuallyExclusiveGroup group = parser.addMutuallyExclusiveGroup()
                .required(true);

        group.addArgument("--add-config")
                .action(store())
                .type(String.class)
                .metavar("ADD-CONFIG")
                .dest("addConfig")
                .help("The configuration to add to the entity.");

        group.addArgument("--delete-config")
                .action(store())
                .type(String.class)
                .metavar("DELETE-CONFIG")
                .dest("deleteConfig")
                .help("config keys to remove 'k1,k2'");

        return parser;
    }
}
