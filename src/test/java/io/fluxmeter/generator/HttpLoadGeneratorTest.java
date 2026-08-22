package io.fluxmeter.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class HttpLoadGeneratorTest {

    @Test
    void runReportsAcceptedThroughputAndLatencyFromHttpBoundary() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/ingest", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        });
        server.start();

        try {
            var config = new HttpLoadGenerator.Config(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/ingest",
                200,
                Duration.ofMillis(400),
                Duration.ZERO,
                256,
                null,
                1,
                5_000,
                5_000
            );

            var result = HttpLoadGenerator.run(config);

            assertTrue(result.offered() > 0);
            assertEquals(result.offered(), result.completed());
            assertEquals(result.completed(), result.accepted());
            assertEquals(0, result.transportErrors());
            assertTrue(result.achievedEps() > 0);
            assertTrue(result.p50Millis() >= 0);
            assertTrue(result.p99Millis() >= result.p50Millis());
            assertTrue(result.meetsGates(config));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void gateFailsWhenAcceptedRateIsBelowMinimum() {
        var config = new HttpLoadGenerator.Config(
            "http://127.0.0.1/ingest",
            10_000,
            Duration.ofSeconds(1),
            Duration.ZERO,
            1,
            null,
            10_000,
            25,
            100
        );
        var result = new HttpLoadGenerator.Result(100, 100, 100, 0, 0, 0, 100, 5, 10, 20);

        assertFalse(result.meetsGates(config));
    }
}
