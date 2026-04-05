package ru.ukhanov.grpc;

import io.tarantool.client.TarantoolClient;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;
import java.util.*;
import java.util.function.BiConsumer;

public class DataStoreClient {
    private static final int PAGE_SIZE = 1000;
    private final TarantoolClient connection;

    public static class LookupResult {
        public final boolean found;
        public final byte[] bytes;
        public LookupResult(boolean ok, byte[] data) {
            this.found = ok;
            this.bytes = data;
        }
    }

    public DataStoreClient(String node, int socket, String login, String pass) throws Exception {
        var group = InstanceConnectionGroup.builder()
                .withHost(node).withPort(socket)
                .withUser(login).withPassword(pass)
                .build();
        this.connection = TarantoolFactory.box()
                .withGroups(List.of(group))
                .build();
    }

    public void insert(String id, byte[] content) {
        connection.eval("box.space.KV:replace({...})", Arrays.asList(id, content)).join();
    }

    public LookupResult lookup(String id) {
        var raw = connection.eval(
                "local x = box.space.KV:get(...) if x == nil then return nil end return x[1], x[2]",
                List.of(id), Object.class
        ).join().get();

        if (raw == null || raw.isEmpty() || raw.get(0) == null) {
            return new LookupResult(false, null);
        }

        byte[] data = raw.size() > 1 && raw.get(1) instanceof byte[] ? (byte[]) raw.get(1) : null;
        return new LookupResult(true, data);
    }

    public void erase(String id) {
        connection.eval("box.space.KV:delete(...)", List.of(id)).join();
    }

    public void iterate(String from, String to, BiConsumer<String, byte[]> handler) throws Exception {
        String script =
                "local a,b,limit,it = ... local out = {} " +
                        "for _,r in box.space.KV.index.primary:pairs(a, {iterator=it}) do " +
                        "   if r[1] > b then break end table.insert(out, {r[1], r[2]}) " +
                        "   if #out >= limit then break end end return unpack(out)";

        String cursor = from;
        boolean first = true;

        while (true) {
            String mode = first ? "GE" : "GT";
            first = false;

            var page = connection.eval(script, Arrays.asList(cursor, to, PAGE_SIZE, mode), Object.class)
                    .join().get();

            if (page == null || page.isEmpty()) break;

            int cnt = 0;
            for (Object item : page) {
                if (!(item instanceof List<?> pair) || pair.isEmpty()) continue;
                String key = pair.get(0).toString();
                if (key.compareTo(to) > 0) return;

                byte[] val = pair.size() > 1 && pair.get(1) instanceof byte[] b ? b : null;
                handler.accept(key, val);
                cursor = key;
                cnt++;
            }
            if (cnt < PAGE_SIZE) break;
        }
    }

    public long size() {
        var result = connection.eval("return box.space.KV:count()", List.of(), Object.class)
                .join().get();
        return ((Number) result.get(0)).longValue();
    }

    public void close() throws Exception {
        if (connection != null) connection.close();
    }
}