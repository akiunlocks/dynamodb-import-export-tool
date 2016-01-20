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
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Takes in SegmentedScanResults and launches several DynamoDBConsumerWorker for
 * each batch of items to write to a DynamoDB table.
 */
public class DynamoDBConsumer extends AbstractLogConsumer {

    private static final String HASH_KEY = "u";
    private static final String RANGE_KEY = "t";
    private final AmazonDynamoDBClient client;
    private final String tableName;
    private final long cutOffDayMillis;
    private final RateLimiter rateLimiter;
    private boolean readOnly;

    /**
     * Class to consume logs and write them to a DynamoDB table.
     */
    public DynamoDBConsumer(AmazonDynamoDBClient client, String tableName,
                            double rateLimit, ExecutorService exec, long cutOffDayMillis, boolean readOnly) {
        this.client = client;
        this.tableName = tableName;
        this.cutOffDayMillis = cutOffDayMillis;
        this.readOnly = readOnly;
        this.rateLimiter = RateLimiter.create(rateLimit);
        threadPool = exec;
        this.exec = new ExecutorCompletionService<Void>(threadPool);
    }

    /**
     * calls splitResultIntoBatches to turn the SegmentedScanResult into several
     * BatchWriteItemRequests and then submits them as individual jobs to the
     * ExecutorService.
     */
    @Override
    public Future<Void> writeResult(SegmentedScanResult result) {
        Future<Void> jobSubmission = null;
        List<BatchWriteItemRequest> batches = splitResultIntoBatches(
                result.getScanResult(), tableName);
        Iterator<BatchWriteItemRequest> batchesIterator = batches.iterator();
        while (batchesIterator.hasNext()) {
            try {
                jobSubmission = exec
                        .submit(new DynamoDBConsumerWorker(batchesIterator
                                .next(), client, rateLimiter, tableName, readOnly));
            } catch (NullPointerException npe) {
                throw new NullPointerException(
                        "Thread pool not initialized for LogStashExecutor");
            }
        }
        return jobSubmission;
    }

    /**
     * Splits up a ScanResult into a list of BatchWriteItemRequests of size 25
     * items or less each.
     */
    public List<BatchWriteItemRequest> splitResultIntoBatches(
            ScanResult result, String tableName) {
        List<BatchWriteItemRequest> batches = new LinkedList<BatchWriteItemRequest>();
        Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();

        BatchWriteItemRequest req = new BatchWriteItemRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        List<WriteRequest> writeRequests = new LinkedList<WriteRequest>();
        int i = 0;
        while (it.hasNext()) {

            Map<String, AttributeValue> item = it.next();

            long ts = Long.parseLong(item.get("t").getN());

            if (ts >= cutOffDayMillis) {
                continue;
            }
            DeleteRequest deleteRequest = new DeleteRequest(ImmutableMap.of(HASH_KEY, new AttributeValue().withS(item.get(HASH_KEY).getS()),
                            RANGE_KEY, new AttributeValue().withN(item.get(RANGE_KEY).getN())));

            writeRequests.add(new WriteRequest(deleteRequest));

            i++;
            if (i == BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM) {
                req.addRequestItemsEntry(tableName, writeRequests);
                batches.add(req);
                req = new BatchWriteItemRequest()
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                writeRequests = new LinkedList<WriteRequest>();
                i = 0;
            }
        }
        if (i > 0) {
            req.addRequestItemsEntry(tableName, writeRequests);
            batches.add(req);
        }
        return batches;
    }
}