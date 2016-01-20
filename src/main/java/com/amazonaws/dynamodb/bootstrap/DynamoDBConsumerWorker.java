/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Callable class that is used to write a batch of items to DynamoDB with exponential backoff.
 */
public class DynamoDBConsumerWorker implements Callable<Void> {
    private static final Logger LOGGER = LogManager.getLogger(DynamoDBConsumerWorker.class);


    private final AmazonDynamoDBClient client;
    private final RateLimiter rateLimiter;
    private long exponentialBackoffTime;
    private BatchWriteItemRequest batch;
    private final String tableName;

    private static final Meter rateMeter = new Meter();

    static Timer timer = new Timer();

    static {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                LOGGER.info("1min write rate:" + rateMeter.getOneMinuteRate() + "; updatesSoFar: " + rateMeter.getCount());
            }
        }, 1000l, 1000l);
    }

    private boolean readOnly;

    /**
     * Callable class that when called will try to write a batch to a DynamoDB
     * table. If the write returns unprocessed items it will exponentially back
     * off until it succeeds.
     */
    public DynamoDBConsumerWorker(BatchWriteItemRequest batchWriteItemRequest,
                                  AmazonDynamoDBClient client, RateLimiter rateLimiter,
                                  String tableName, boolean readOnly) {
        this.batch = batchWriteItemRequest;
        this.client = client;
        this.rateLimiter = rateLimiter;
        this.tableName = tableName;
        this.readOnly = readOnly;
        this.exponentialBackoffTime = BootstrapConstants.INITIAL_RETRY_TIME_MILLISECONDS;
    }

    /**
     * Batch writes the write request to the DynamoDB endpoint and THEN acquires
     * permits equal to the consumed capacity of the write.
     */
    @Override
    public Void call() {
        if (readOnly) {
            rateLimiter.acquire(BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM);
            batch.getRequestItems().forEach(new BiConsumer<String, List<WriteRequest>>() {
                @Override
                public void accept(String s, List<WriteRequest> writeRequests) {
                    rateMeter.mark(writeRequests.size());
                }
            });
            return null;
        }

        List<ConsumedCapacity> batchResult = runWithBackoff(batch);
        Iterator<ConsumedCapacity> it = batchResult.iterator();
        int consumedCapacity = 0;
        while (it.hasNext()) {
            consumedCapacity += it.next().getCapacityUnits().intValue();
        }
        rateLimiter.acquire(consumedCapacity);
        return null;
    }

    /**
     * Writes to DynamoDBTable using an exponential backoff. If the
     * batchWriteItem returns unprocessed items then it will exponentially
     * backoff and retry the unprocessed items.
     */
    public List<ConsumedCapacity> runWithBackoff(BatchWriteItemRequest req) {
        BatchWriteItemResult writeItemResult = null;
        List<ConsumedCapacity> consumedCapacities = new LinkedList<ConsumedCapacity>();
        Map<String, List<WriteRequest>> unprocessedItems = null;
        boolean interrupted = false;
        try {
            do {
                writeItemResult = client.batchWriteItem(req);
                unprocessedItems = writeItemResult.getUnprocessedItems();
                consumedCapacities
                        .addAll(writeItemResult.getConsumedCapacity());

                rateMeter.mark(req.getRequestItems().get(tableName).size());

                unprocessedItems.remove(tableName);

                if (unprocessedItems != null) {
                    req.setRequestItems(unprocessedItems);
                    try {
                        Thread.sleep(exponentialBackoffTime);
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime *= 2;
                        if (exponentialBackoffTime > BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME) {
                            exponentialBackoffTime = BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME;
                        }
                    }
                }
            } while (unprocessedItems.get(tableName) != null);

            return consumedCapacities;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
