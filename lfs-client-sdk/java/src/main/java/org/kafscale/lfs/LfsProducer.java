// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.kafscale.lfs;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LfsProducer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient client;
    private final URI endpoint;

    public LfsProducer(URI endpoint) {
        this.client = HttpClient.newHttpClient();
        this.endpoint = endpoint;
    }

    public LfsEnvelope produce(String topic, byte[] key, InputStream payload, Map<String, String> headers) throws Exception {
        Map<String, String> outHeaders = new HashMap<>();
        outHeaders.put("X-Kafka-Topic", topic);
        if (key != null) {
            outHeaders.put("X-Kafka-Key", new String(key));
        }
        if (headers != null) {
            outHeaders.putAll(headers);
        }

        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(Duration.ofMinutes(5))
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> payload));

        for (Map.Entry<String, String> entry : outHeaders.entrySet()) {
            req.header(entry.getKey(), entry.getValue());
        }

        HttpResponse<String> resp = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("produce failed: " + resp.statusCode() + " " + resp.body());
        }
        return MAPPER.readValue(resp.body(), LfsEnvelope.class);
    }
}
