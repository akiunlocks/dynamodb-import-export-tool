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

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.beust.jcommander.JCommander;
import org.apache.log4j.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * The interface that parses the arguments, and begins to transfer data from one
 * DynamoDB table to another
 */
public class CommandLineInterface {

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(CommandLineInterface.class);



    /**
     * Main class to begin transferring data from one DynamoDB table to another
     * DynamoDB table.
     *
     * @param args
     */
    public static void main(String[] args) {


//        GenericInit.eyeview();



        BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n")));
        Logger.getRootLogger().setLevel(Level.INFO);

        try {
            Logger.getRootLogger().addAppender(new DailyRollingFileAppender(new PatternLayout("%d{HH:mm:ss.SSS} [%t] %-5p %30.30c - %m%n"), "/mnt/log/dynamodb-import-export-tool.log", "yyyy-MM-dd"));
        } catch (IOException e) {
            LOGGER.warn("failed to add file appender", e);
        }


        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        // parse given arguments
        cmd.parse(args);

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            return;
        }

        final String sourceEndpoint = params.getSourceEndpoint();
        final String destinationEndpoint = params.getDestinationEndpoint();
        final String destinationTable = params.getDestinationTable();
        final String sourceTable = params.getSourceTable();
        final double readThroughputRatio = params.getReadThroughputRatio();
        final double writeThroughputRatio = params.getWriteThroughputRatio();
        final int maxWriteThreads = params.getMaxWriteThreads();
        final boolean consistentScan = params.getConsistentScan();
        final int daysToKeep = params.getDaysToKeep();
        final boolean readOnly = params.getReadOnly();

        LOGGER.info("sourceEndpoint = " + params.getSourceEndpoint());
        LOGGER.info("destinationEndpoint = " + params.getDestinationEndpoint());
        LOGGER.info("destinationTable = " + params.getDestinationTable());
        LOGGER.info("sourceTable = " + params.getSourceTable());
        LOGGER.info("readThroughputRatio = " + params.getReadThroughputRatio());
        LOGGER.info("writeThroughputRatio = " + params.getWriteThroughputRatio());
        LOGGER.info("maxWriteThreads = " + params.getMaxWriteThreads());
        LOGGER.info("consistentScan = " + params.getConsistentScan());
        LOGGER.info("daysToKeep = " + params.getDaysToKeep());
        LOGGER.info("readOnly = " + params.getReadOnly());


        final DateTime cutOffDay = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay().minusDays(daysToKeep);
        long cutOffDayMillis = cutOffDay.getMillis();

        final AmazonDynamoDBClient sourceClient = new AmazonDynamoDBClient(
                new EVAWSCredentialsProviderChain());
        final AmazonDynamoDBClient destinationClient = new AmazonDynamoDBClient(
                new EVAWSCredentialsProviderChain());
        sourceClient.setEndpoint(sourceEndpoint);
        destinationClient.setEndpoint(destinationEndpoint);

        TableDescription readTableDescription = sourceClient.describeTable(
                sourceTable).getTable();
        TableDescription writeTableDescription = destinationClient
                .describeTable(destinationTable).getTable();
        int numSegments = 10;
        try {
            numSegments = DynamoDBBootstrapWorker
                    .getNumberOfSegments(readTableDescription);
            LOGGER.info("Number segments : " + numSegments);
        } catch (NullReadCapacityException e) {
            LOGGER.warn("Number of segments not specified - defaulting to "
                    + numSegments, e);
        }

        final double readThroughput = calculateThroughput(readTableDescription,
                readThroughputRatio, true);
        LOGGER.info("readThroughput = " + readThroughput);
        final double writeThroughput = calculateThroughput(
                writeTableDescription, writeThroughputRatio, false);
        LOGGER.info("writeThroughput = " + writeThroughput);

        try {
            ExecutorService sourceExec = getSourceThreadPool(numSegments);
            ExecutorService destinationExec = getDestinationThreadPool(maxWriteThreads);
            DynamoDBConsumer consumer = new DynamoDBConsumer(destinationClient,
                    destinationTable, writeThroughput, destinationExec, cutOffDayMillis, readOnly);

            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(
                    sourceClient, readThroughput, sourceTable, sourceExec,
                    params.getSection(), params.getTotalSections(), numSegments, consistentScan);

            LOGGER.info("Starting transfer...");
            worker.pipe(consumer);
            LOGGER.info("Finished Copying Table.");
        } catch (ExecutionException e) {
            LOGGER.error("Encountered exception when executing transfer.", e);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when executing transfer.", e);
            System.exit(1);
        } catch (SectionOutOfRangeException e) {
            LOGGER.error("Invalid section parameter", e);
        }
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     */
    private static double calculateThroughput(
            TableDescription tableDescription, double throughputRatio,
            boolean read) {
        if (read) {
            return tableDescription.getProvisionedThroughput()
                    .getReadCapacityUnits() * throughputRatio;
        }
        return tableDescription.getProvisionedThroughput()
                .getWriteCapacityUnits() * throughputRatio;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    public static ExecutorService getDestinationThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }
        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                maxWriteThreads, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(maxWriteThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

    /**
     * Returns the thread pool for the source DynamoDB table.
     */
    private static ExecutorService getSourceThreadPool(int numSegments) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > numSegments) {
            corePoolSize = numSegments - 1;
        }

        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                numSegments, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(numSegments),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

    static class EVAWSCredentialsProviderChain extends AWSCredentialsProviderChain {
        public EVAWSCredentialsProviderChain() {
            super(new EnvironmentVariableCredentialsProvider(),
                    new PropertiesFileCredentialsProvider("/etc/eyeview/aws.credentials"),
                    new SystemPropertiesCredentialsProvider(),
                    new ProfileCredentialsProvider(),
                    new InstanceProfileCredentialsProvider());
        }
    }
}