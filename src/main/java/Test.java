import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.relational.core.mapping.Column;

public class Test {

    public static void main(String[] args) throws IOException {
        PostgresqlConnectionConfiguration.Builder builder =
                PostgresqlConnectionConfiguration.builder().database("identity").username("identity").password("identity");

        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(builder.host("127.0.0.1").port(5432).build());

        boolean pooling = true;
        ConnectionFactory pooledConnectionFactory = pooling ? simulatePooler(connectionFactory.create()) : connectionFactory;

        DatabaseClient dbc = DatabaseClient.create(pooledConnectionFactory);

        List<String> res = new ArrayList<>();

        NumberFormat f = NumberFormat.getInstance();

        for (int i = 0; i < 100; i++) {
            long start = System.nanoTime();
            dbc.select().from("oauth.client").matching(Criteria.where("key").is("tucanoTrustedUser")).as(Client.class).fetch().first().blockOptional();
            res.add((pooling ? "pooled" : "direct") + " - run " + i + " - " + f.format(System.nanoTime() - start));
        }

        for (String re : res) {
            System.out.println(re);
        }
    }

    private static ConnectionFactory simulatePooler(Mono<? extends Connection> connectionMono) {
        Mono<? extends Connection> pooled = connectionMono.map(connection -> new Connection() {
            @Override
            public Publisher<Void> beginTransaction() {
                return connection.beginTransaction();
            }

            @Override
            public Publisher<Void> close() {
                return Mono.empty();
            }

            @Override
            public Publisher<Void> commitTransaction() {
                return connection.commitTransaction();
            }

            @Override
            public Batch createBatch() {
                return connection.createBatch();
            }

            @Override
            public Publisher<Void> createSavepoint(String s) {
                return connection.createSavepoint(s);
            }

            @Override
            public Statement createStatement(String s) {
                return connection.createStatement(s);
            }

            @Override
            public boolean isAutoCommit() {
                return connection.isAutoCommit();
            }

            @Override
            public ConnectionMetadata getMetadata() {
                return connection.getMetadata();
            }

            @Override
            public IsolationLevel getTransactionIsolationLevel() {
                return connection.getTransactionIsolationLevel();
            }

            @Override
            public Publisher<Void> releaseSavepoint(String s) {
                return connection.releaseSavepoint(s);
            }

            @Override
            public Publisher<Void> rollbackTransaction() {
                return connection.rollbackTransaction();
            }

            @Override
            public Publisher<Void> rollbackTransactionToSavepoint(String s) {
                return connection.rollbackTransactionToSavepoint(s);
            }

            @Override
            public Publisher<Void> setAutoCommit(boolean b) {
                return connection.setAutoCommit(b);
            }

            @Override
            public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
                return connection.setTransactionIsolationLevel(isolationLevel);
            }

            @Override
            public Publisher<Boolean> validate(ValidationDepth validationDepth) {
                return connection.validate(validationDepth);
            }
        }).cache();

        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return pooled;
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    public static class Client {

        @Column("id_client")
        public Long idClient;

        @Column("key")
        public String key;

    }
}
