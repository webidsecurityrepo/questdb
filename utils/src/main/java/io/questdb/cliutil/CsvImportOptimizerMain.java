/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cliutil;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CsvImportOptimizerMain {

    private static final int READ_BUFFER_SIZE = 4 * 1024 * 1024; // 4MB

    public static void main(String[] args) throws IOException {
        CommandLineParams params = parseCommandParams(args);
        if (params == null) {
            // Invalid params, usage already printed
            return;
        }

        if (!validateCommandParams(params)) {
            printUsage();
            return;
        }

        if (params.debug) {
            System.out.println("Invoked with the following parameters: " + params);
        }

        AWSCredentials credentials;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            System.err.println("Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.");
            if (params.debug) {
                e.printStackTrace();
            }
            return;
        }

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(params.region)
                .build();

        try {
            S3Object object = s3.getObject(new GetObjectRequest(params.inputBucket, params.inputFile));
            // TODO validate object.getObjectMetadata().getContentType()

            printFileContents(object.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.err.println("Faced an Amazon service error.");
            System.err.println("Error Message:    " + ase.getMessage());
            System.err.println("HTTP Status Code: " + ase.getStatusCode());
            System.err.println("AWS Error Code:   " + ase.getErrorCode());
            System.err.println("Error Type:       " + ase.getErrorType());
            System.err.println("Request ID:       " + ase.getRequestId());
            if (params.debug) {
                ase.printStackTrace();
            }
        } catch (AmazonClientException ace) {
            System.err.println("Faced an Amazon SDK client error. Error Message: " + ace.getMessage());
            if (params.debug) {
                ace.printStackTrace();
            }
        }
    }

    private static CommandLineParams parseCommandParams(String[] args) {
        if (args.length > 7) {
            printUsage();
            return null;
        }

        CommandLineParams res = new CommandLineParams();
        for (int i = 0, n = args.length; i < n;) {
            if ("--region".equals(args[i])) {
                if (res.region == null) {
                    res.region = args[i + 1];
                    i++;
                } else {
                    System.err.println("--region parameter can be only used once");
                    printUsage();
                    return null;
                }
            }

            if ("--input-bucket".equals(args[i])) {
                if (res.inputBucket == null) {
                    res.inputBucket = args[i + 1];
                    i++;
                } else {
                    System.err.println("--input-bucket parameter can be only used once");
                    printUsage();
                    return null;
                }
            }

            if ("--input-file".equals(args[i])) {
                if (res.inputFile == null) {
                    res.inputFile = args[i + 1];
                    i++;
                } else {
                    System.err.println("--input-file parameter can be only used once");
                    printUsage();
                    return null;
                }
            }

            if ("--debug".equals(args[i])) {
                if (!res.debug) {
                    res.debug = true;
                }
            }

            i++;
        }

        return res;
    }

    private static boolean validateCommandParams(CommandLineParams params) {
        if (params.region == null) {
            System.err.println("--region parameter is not specified");
            return false;
        }
        if (params.inputBucket == null) {
            System.err.println("--input-bucket parameter is not specified");
            return false;
        }
        if (params.inputFile == null) {
            System.err.println("--input-file parameter is not specified");
            return false;
        }
        return true;
    }

    private static void printUsage() {
        System.out.println("usage: " + CsvImportOptimizerMain.class.getName()
                + " --region <aws_region> --input-bucket <s3_input_bucket_name> --input-file <s3_input_file> [--debug]");
    }

    private static void printFileContents(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input), READ_BUFFER_SIZE);
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    private static class CommandLineParams {

        boolean debug;
        String region;
        String inputBucket;
        String inputFile;

        @Override
        public String toString() {
            return "CommandLineParams{" +
                    "region='" + region + '\'' +
                    ", inputBucket='" + inputBucket + '\'' +
                    ", inputFile='" + inputFile + '\'' +
                    '}';
        }
    }
}
