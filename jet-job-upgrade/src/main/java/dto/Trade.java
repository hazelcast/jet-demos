package dto;/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Serializable;

/**
 * We use java.io.{@link Serializable} here for the sake of simplicity.
 * In production, Hazelcast Custom Serialization should be used.
 */
public class Trade implements Serializable {

    private final long time;
    private final String ticker;
    private final int quantity;
    private final int price; // in cents

    public Trade(long time, String ticker, int quantity, int price) {
        this.time = time;
        this.ticker = ticker;
        this.quantity = quantity;
        this.price = price;
    }

    public Trade(String line) {
        String[] args = line.split(" ");
        this.time = Long.parseLong(args[0]);
        this.ticker = args[1];
        this.quantity = Integer.parseInt(args[2]);
        this.price = Integer.parseInt(args[3]);
    }

    /**
     * Timestamp for the trade in UNIX time
     */
    public long getTime() {
        return time;
    }

    /**
     * The ticker
     */
    public String getSymbol() {
        return ticker;
    }

    public int getQuantity() {
        return quantity;
    }

    /**
     * The price in cents
     */
    public int getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return String.format("%d %s %d %d", time, ticker, quantity, price);
    }
}