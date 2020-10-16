import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

public class Test {

    public static void main(String[] args) throws IOException {
        PostgresqlConnectionConfiguration.Builder builder =
                PostgresqlConnectionConfiguration.builder().database("identity").username("identity").password("identity");

        PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(builder.host("127.0.0.1").port(5432).build());

        List<String> res = new ArrayList<>();
        NumberFormat f = NumberFormat.getInstance();
        Connection c = Mono.from(connectionFactory.create()).block();

        for (int i = 0; i < 10; i++) {
            long start = System.nanoTime();
            Flux<String> flux = Flux
                    .from(c.createStatement("SELECT * FROM oauth.client WHERE key = $1").bind("$1", "testRC123").execute())
                    .flatMap(result -> result.map((row, metadata) -> "dummy"));
            flux.blockLast();
            res.add("run " + i + " - " + f.format(System.nanoTime() - start));
        }

        for (String re : res) {
            System.out.println(re);
        }
    }
}
