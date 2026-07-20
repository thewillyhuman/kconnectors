/*
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.thewillyhuman.kafka.connect.source.amq;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the {@code amq.url} broker list. Entries are separated by top-level commas;
 * commas inside parentheses belong to Qpid failover URIs
 * (e.g. {@code failover:(amqp://a:61112,amqp://b:61112)}) and are left alone.
 */
final class BrokerUrls {

    private BrokerUrls() {
    }

    static List<String> parse(String value) {
        List<String> urls = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth < 0) {
                    throw new IllegalArgumentException("Unbalanced parentheses in broker list");
                }
            } else if (c == ',' && depth == 0) {
                urls.add(entry(value, start, i));
                start = i + 1;
            }
        }
        if (depth != 0) {
            throw new IllegalArgumentException("Unbalanced parentheses in broker list");
        }
        urls.add(entry(value, start, value.length()));
        return List.copyOf(urls);
    }

    private static String entry(String value, int start, int end) {
        String url = value.substring(start, end).trim();
        if (url.isEmpty()) {
            throw new IllegalArgumentException("Empty broker URL in list");
        }
        return url;
    }
}
