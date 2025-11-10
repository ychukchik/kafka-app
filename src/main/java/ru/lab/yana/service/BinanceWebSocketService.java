package ru.lab.yana.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;

@Component
@RequiredArgsConstructor
public class BinanceWebSocketService {

    private static final String URL = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    private final BinanceWebSocketListener binanceWebSocketListener;

    @PostConstruct
    public void connect() {
        HttpClient.newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create(URL), binanceWebSocketListener);

        System.out.println("Binance WebSocket client started");
    }
}