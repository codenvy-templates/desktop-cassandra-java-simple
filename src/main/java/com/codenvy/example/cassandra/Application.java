package com.codenvy.example.cassandra;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class Application {
    private final static String           KEYSPACE    = "Keyspace1";
    private final static String           HOST_PORT   = "localhost:9160";
    private final static String           CF_NAME     = "Standard1";
    /** Column name where values are stored */
    private final static String           COLUMN_NAME = "v";
    private final        StringSerializer serializer  = StringSerializer.get();

    private final Keyspace keyspace;

    public static void main(String[] args) throws HectorException {
        Cluster c = getOrCreateCluster("MyCluster", HOST_PORT);
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, CF_NAME, ComparatorType.BYTESTYPE);
        KeyspaceDefinition newKeyspace =
                HFactory.createKeyspaceDefinition(KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));

        c.addKeyspace(newKeyspace, true);

        Application app = new Application(createKeyspace(KEYSPACE, c));

        final String key1 = "key1";
        final String value1 = "value1";
        System.out.println(String.format("Insert into storage single keyValue: %s=%s", key1, value1));
        app.insert(key1, value1, StringSerializer.get());
        System.out.println(String.format("Get value for key %s: %s", key1, app.get(key1, StringSerializer.get())));

        HashMap<String, String> keyValues = new HashMap<>();
        keyValues.put("mapKey1", "mapValue1");
        keyValues.put("mapKey2", "mapValue2");
        System.out.println(String.format("Insert into storage multiValue map: %s", keyValues.toString()));
        app.insertMulti(keyValues, StringSerializer.get());

        System.out.println(String.format("Get values for keys %s: %s", keyValues.keySet(),
                                         app.getMulti(StringSerializer.get(), "mapKey1", "mapKey2")));

        System.out.println("Deleting from storage mapKey1");
        app.delete(StringSerializer.get(), "mapKey1");

        System.out.println("Value for key mapKey1 doesn't exist: " + (app.get("mapKey1", StringSerializer.get()) == null));
    }

    public Application(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * Insert a new value keyed by key
     */
    public <K> void insert(final K key, final String value, Serializer<K> keySerializer) {
        createMutator(keyspace, keySerializer).insert(
                key, CF_NAME, createColumn(COLUMN_NAME, value, serializer, serializer));
    }

    /**
     * Get a string value.
     */
    public <K> String get(final K key, Serializer<K> keySerializer) throws HectorException {
        ColumnQuery<K, String, String> q = createColumnQuery(keyspace, keySerializer, serializer, serializer);
        QueryResult<HColumn<String, String>> r = q.setKey(key).
                setName(COLUMN_NAME).
                                                          setColumnFamily(CF_NAME).
                                                          execute();
        HColumn<String, String> c = r.get();
        return c == null ? null : c.getValue();
    }

    /**
     * Get multiple values
     */
    @SafeVarargs
    public final <K> Map<K, String> getMulti(Serializer<K> keySerializer, K... keys) {
        MultigetSliceQuery<K, String, String> q = createMultigetSliceQuery(keyspace, keySerializer, serializer, serializer);
        q.setColumnFamily(CF_NAME);
        q.setKeys(keys);
        q.setColumnNames(COLUMN_NAME);

        QueryResult<Rows<K, String, String>> r = q.execute();
        Rows<K, String, String> rows = r.get();
        Map<K, String> ret = new HashMap<>(keys.length);
        for (K k : keys) {
            HColumn<String, String> c = rows.getByKey(k).getColumnSlice().getColumnByName(COLUMN_NAME);
            if (c != null && c.getValue() != null) {
                ret.put(k, c.getValue());
            }
        }
        return ret;
    }

    /**
     * Insert multiple values
     */
    public <K> void insertMulti(Map<K, String> keyValues, Serializer<K> keySerializer) {
        Mutator<K> m = createMutator(keyspace, keySerializer);
        for (Map.Entry<K, String> keyValue : keyValues.entrySet()) {
            m.addInsertion(keyValue.getKey(), CF_NAME,
                           createColumn(COLUMN_NAME, keyValue.getValue(), keyspace.createClock(), serializer, serializer));
        }
        m.execute();
    }

    /**
     * Delete multiple values
     */
    @SafeVarargs
    public final <K> void delete(Serializer<K> keySerializer, K... keys) {
        Mutator<K> m = createMutator(keyspace, keySerializer);
        for (K key : keys) {
            m.addDeletion(key, CF_NAME, COLUMN_NAME, serializer);
        }
        m.execute();
    }
}
