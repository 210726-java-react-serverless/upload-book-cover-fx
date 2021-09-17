package com.revature.cover_upload;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.fileupload.MultipartStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

public class BookCoverUploadHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Gson mapper = new GsonBuilder().setPrettyPrinting().create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion("us-west-1").build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent requestEvent, Context context) {

        LambdaLogger logger = context.getLogger();
        logger.log("Request received at " + LocalDateTime.now());

        APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();

        try {

            byte[] fileByteArray = requestEvent.getBody().getBytes();

            logger.log("Incoming request body size: " + fileByteArray.length);

            boolean isBase64Encoded = requestEvent.getIsBase64Encoded();
            logger.log("Request is Base64 encoded: " + isBase64Encoded);

            if (!isBase64Encoded) {
                logger.log("Invalid request, expected body to be Base 64 encoded.");
                responseEvent.setStatusCode(400);
                return responseEvent;
            }

            logger.log("Decoding file byte array");
            byte[] decodedFileByteArray = Base64.getDecoder().decode(fileByteArray);


            logger.log("Retrieving content-type header value and extract boundary");
            Map<String, String> reqHeaders = requestEvent.getHeaders();

            if (reqHeaders == null || !reqHeaders.containsKey("Content-Type")) {
                logger.log("Could not process request due to missing content-type header.");
                responseEvent.setStatusCode(400);
                return responseEvent;
            }

            String contentType = reqHeaders.get("Content-Type");
            byte[] boundary = contentType.split("=")[1].getBytes(StandardCharsets.UTF_8);

            logger.log("Content-type and boundary extracted from request");
            logger.log("Decoded file byte array: " + new String(decodedFileByteArray, StandardCharsets.UTF_8) + "\n");


            logger.log("Writing file data to byte stream");
            ByteArrayInputStream content = new ByteArrayInputStream(decodedFileByteArray);
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            MultipartStream multipartStream = new MultipartStream(content, boundary, decodedFileByteArray.length, null);

            boolean hasNext = multipartStream.skipPreamble();

            while (hasNext) {
                String header = multipartStream.readHeaders();
                logger.log("Headers: " + header);
                multipartStream.readBodyData(output);
                hasNext = multipartStream.readBoundary();
            }

            logger.log("File data written to byte stream");

            String bucketName = "bookstore-images-bucket";
            logger.log("Preparing file for persistence to S3 bucket: " + bucketName);
            InputStream inStream = new ByteArrayInputStream(output.toByteArray());

            ObjectMetadata objMetadata = new ObjectMetadata();
            objMetadata.setContentLength(output.toByteArray().length);
            objMetadata.setContentType(contentType);

            String uniqueFileName = UUID.randomUUID().toString();
            PutObjectResult result = s3Client.putObject(bucketName, uniqueFileName, inStream, objMetadata);

            logger.log("File successfully persisted to S3 bucket");
            logger.log("Result: " + result);

            logger.log("Preparing response object");
            responseEvent.setStatusCode(201);

            Map<String, String> respBody = new HashMap<>();
            respBody.put("status", "uploaded");
            respBody.put("uuid", uniqueFileName);
            responseEvent.setBody(mapper.toJson(respBody));


        } catch(Exception e) {
            responseEvent.setStatusCode(500);
            logger.log("An unexpected exception occurred: " + e.getMessage());
        }

        logger.log("Request processing complete, sending response: " + responseEvent);
        return responseEvent;

    }


}
