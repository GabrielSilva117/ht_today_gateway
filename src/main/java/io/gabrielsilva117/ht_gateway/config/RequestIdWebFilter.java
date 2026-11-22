package io.gabrielsilva117.ht_gateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Ensures every request carries an X-Request-Id header before it reaches a downstream
 * service, so log lines across the gateway -> service hop can be correlated in Loki.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdWebFilter implements WebFilter {

    private static final String HEADER = "X-Request-Id";
    private static final Logger log = LoggerFactory.getLogger(RequestIdWebFilter.class);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String requestId = exchange.getRequest().getHeaders().getFirst(HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }

        ServerHttpRequest mutatedRequest = exchange.getRequest().mutate()
                .header(HEADER, requestId)
                .build();
        exchange.getResponse().getHeaders().add(HEADER, requestId);

        MDC.put("requestId", requestId);
        try {
            log.info("{} {}", exchange.getRequest().getMethod(), exchange.getRequest().getPath());
        } finally {
            MDC.remove("requestId");
        }

        return chain.filter(exchange.mutate().request(mutatedRequest).build());
    }
}
