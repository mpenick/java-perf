package com.datastax;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by michaelpenick on 9/8/16.
 */
public class Perf {

    static final String KEYSPACE_SCHEMA =
            "CREATE KEYSPACE IF NOT EXISTS perf WITH " +
                    "replication = { 'class': 'NetworkTopologyStrategy', 'dc1' : 2, 'dc2' : 3 }";

    static final String TABLE_SCHEMA =
            "CREATE TABLE IF NOT EXISTS "  +
                    "perf.table1 (key uuid PRIMARY KEY, value varchar)";

    static final String TRUNCATE_TABLE =
            "TRUNCATE perf.table1";

    static final String SELECT_QUERY =
            "SELECT * FROM perf.table1 WHERE key = ?";

    static final String PRIMING_INSERT_QUERY =
            "INSERT INTO perf.table1 (key, value) VALUES (?, ?)";

    static final String INSERT_QUERY =
            "INSERT INTO perf.table1 (key, value) VALUES (?, ?)";

    static final String DATA = "a";


    static final int NUM_THREADS = 1;
    static final int NUM_REQUESTS = 10000;
    static final int NUM_CONCURRENT_REQUESTS = 10000;

    public static class TrustAllX509TrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
        }

        public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
                                       String authType) {
       }
    }

    public static class PrintRetryPolicy implements RetryPolicy {
        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            System.err.println("############################################### Read Timeout");
            return DefaultRetryPolicy.INSTANCE.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            System.err.println("############################################### Write Timeout");
            return DefaultRetryPolicy.INSTANCE.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            System.err.println("############################################### Unavailble");
            return DefaultRetryPolicy.INSTANCE.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
        }

        @Override
        public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
            return DefaultRetryPolicy.INSTANCE.onRequestError(statement, cl, e, nbRetry);
        }

        @Override
        public void init(Cluster cluster) {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    public static class Callback implements FutureCallback<ResultSet> {
        public Callback(Session session, final PreparedStatement prepared, final CountDownLatch latch, final Executor executor) {
            this.session = session;
            this.prepared = prepared;
            this.latch = latch;
            this.executor = executor;
        }

        @Override
        public void onSuccess(ResultSet rows) {
            run();
        }

        @Override
        public void onFailure(Throwable throwable) {
            System.err.printf("Error: %s\n", throwable.getMessage());
            run();
        }

        void run() {
            latch.countDown();
            if (latch.getCount() > 0) {
                BoundStatement statement = prepared.bind(UUIDs.random(), DATA);
                statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
                ResultSetFuture future = session.executeAsync(statement);
                latch.countDown();
                Futures.addCallback(future, new Callback(session, prepared, latch, executor), executor);
            }
        }

        private final Session session;
        private final PreparedStatement prepared;
        private final CountDownLatch latch;
        private final Executor executor;
    }

     public static ArrayList<UUID> prime_select_query_data(Session session, int num_partition_keys, String data) {
         ArrayList<UUID> primary_keys = new ArrayList<>();
         for (int i = 0; i < num_partition_keys; ++i) {
             UUID uuid = UUIDs.random();
             session.execute(PRIMING_INSERT_QUERY, uuid, data);
             primary_keys.add(uuid);
         }
         return primary_keys;
    }

    public static void run(Session session,
                           final PreparedStatement prepared,
                           final int numRequests,
                           final CountDownLatch latch,
                           final Executor executor) {
        int numConcurrentRequests = Math.min(numRequests, NUM_CONCURRENT_REQUESTS);
        for (int i = 0; i < numConcurrentRequests; ++i) {
            BoundStatement statement = prepared.bind(UUIDs.random(), DATA);
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            ResultSetFuture future = session.executeAsync(statement);
            Futures.addCallback(future, new Callback(session, prepared, latch, executor), executor);
        }
    }

    public static void main(String args[]) throws InterruptedException, ExecutionException, NoSuchAlgorithmException, KeyManagementException {
        int numRequests = NUM_REQUESTS;
        int numThreads = NUM_THREADS;
        String contactPoints = "127.0.0.1";
        boolean useSSL = false;

        for (int i = 0; i < args.length; ++i) {
            String arg = args[i].toLowerCase();
            switch(arg) {
                case "--ssl":
                    useSSL = true;
                    break;

                case "--requests":
                    if (i + 1 > args.length) {
                        System.err.println("--requests expects an integer argument");
                        System.exit(-1);
                    }
                    try {
                        numRequests = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        System.err.println("--requests expects an integer argument");
                        System.exit(-1);
                    }
                    i++;
                    break;

                case "--threads":
                    if (i + 1 > args.length) {
                        System.err.println("--threads expects an integer argument");
                        System.exit(-1);
                    }
                    try {
                        numThreads = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        System.err.println("--threads expects an integer argument");
                        System.exit(-1);
                    }
                    i++;
                    break;

                case "--contacts":
                    if (i + 1 > args.length) {
                        System.err.println("--contacts expects a string argument");
                        System.exit(-1);
                    }
                    contactPoints = args[i + 1];
                    i++;
                    break;

                default:
                    System.err.printf("Unexpected argument '%s'\n", arg);
                    System.exit(-1);
                    break;
            }
        }

        System.out.printf("running with %d threads and %d requests (ssl: %s, contacts: %s)\n",
                numThreads, numRequests, (useSSL ? "yes" : "no"), contactPoints);

        final int connectionPerHost = 1;
        final int maxRequestPerConnection = 10000;

        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(contactPoints.split(","))
                .withRetryPolicy(new PrintRetryPolicy())
                .withPoolingOptions(new PoolingOptions()
                        .setConnectionsPerHost(HostDistance.LOCAL, connectionPerHost, connectionPerHost)
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestPerConnection)
                        .setConnectionsPerHost(HostDistance.REMOTE, connectionPerHost, connectionPerHost)
                        .setMaxRequestsPerConnection(HostDistance.REMOTE, maxRequestPerConnection));
        if (useSSL) {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] { new TrustAllX509TrustManager() }, new java.security.SecureRandom());
            JdkSSLOptions sslOptions = JdkSSLOptions.builder().withSSLContext(sc).build();
            builder = builder.withSSL(sslOptions);
        }

        try(Cluster cluster = builder.build()) {
            try (Session session = cluster.connect()) {
                session.execute(KEYSPACE_SCHEMA);
                session.execute(TABLE_SCHEMA);
                try {
                    session.execute(TRUNCATE_TABLE);
                } catch(Exception e) {
                    // Don't care
                }
            }
        }

        builder.withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(500, 1));

        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try(Cluster cluster = builder.build()) {
            try (Session session = cluster.connect()) {
                final CountDownLatch latch = new CountDownLatch(numRequests);
                final PreparedStatement prepared = session.prepare(INSERT_QUERY);
                prepared.setConsistencyLevel(ConsistencyLevel.QUORUM);

                long start = System.currentTimeMillis();
                Perf.run(session, prepared, numRequests, latch, executor);

                while(!latch.await(2, TimeUnit.SECONDS)) {
                    Timer requestsTimer = cluster.getMetrics().getRequestsTimer();
                    System.out.printf("rate stats (requests/second): mean %f 1m %f 5m %f 15m %f\n",
                            requestsTimer.getMeanRate(),
                            requestsTimer.getOneMinuteRate(),
                            requestsTimer.getFiveMinuteRate(),
                            requestsTimer.getFifteenMinuteRate());
                }

                System.out.printf("inserted %d rows in %f seconds\n",
                        numRequests,
                        (System.currentTimeMillis() - start) / 1000.0);

                Snapshot snapshot = cluster.getMetrics().getRequestsTimer().getSnapshot();
                System.out.printf("final stats (microseconds): min %d max %d median %f 75th %f 98th %f 99th %f 99.9th %f\n",
                        snapshot.getMin(),
                        snapshot.getMax(),
                        snapshot.getMedian(),
                        snapshot.get75thPercentile(),
                        snapshot.get98thPercentile(),
                        snapshot.get99thPercentile(),
                        snapshot.get999thPercentile());
            }
        }

        executor.shutdown();
    }
}
