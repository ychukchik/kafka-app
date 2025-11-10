package ru.lab.yana.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.http.WebSocket;
import java.time.LocalDateTime;
import java.util.concurrent.CompletionStage;

@Component
@RequiredArgsConstructor
@Slf4j
public class BinanceWebSocketListener implements WebSocket.Listener {

    private final KafkaProducerService kafkaProducerService;

    @Override
    public void onOpen(WebSocket webSocket) {
        WebSocket.Listener.super.onOpen(webSocket);
        log.info("Connected to Binance WebSocket!");
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        log.info("Received: {}", data);
        processTradeData(data.toString());

        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.info("Connection closed: {}", reason);
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.info("WebSocket error: {}", error.getMessage());
        WebSocket.Listener.super.onError(webSocket, error);
    }

    private void processTradeData(String tradeData) {
        try {
            String key = "binance-trade";

            ObjectMapper mapper = new ObjectMapper();
            JsonNode tradeJson = mapper.readTree(tradeData);
            ((ObjectNode) tradeJson).put("timestamp", LocalDateTime.now().toString());

            String tradeDataEnrichTimestamp = tradeJson.toString();
            log.info("Send message {}", tradeDataEnrichTimestamp);
            kafkaProducerService.sendMessage(key, tradeDataEnrichTimestamp);
            log.info("Message successfully send");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}