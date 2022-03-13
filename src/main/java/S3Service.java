import lombok.Builder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Builder
public class S3Service {

    @Builder.Default
    S3Client s3 = getClient();
    @Builder.Default
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    public static S3Service create() {
        return builder().build();
    }

    private static S3Client getClient() {
        // Create the S3Client object
        Region region = Region.US_EAST_1;
        return S3Client.builder()
                //.credentialsProvider(InstanceProfileCredentialsProvider.builder().build())
                .region(region)
                .build();
    }

    /**
     * Creates a bucket on S3
     */
    public String createBucket(String name) {
        String bucketName;
        try {
            bucketName = (name + "-" + credentialsProvider.resolveCredentials().accessKeyId()).toLowerCase();
            S3Waiter s3Waiter = s3.waiter();
            var bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build();

            s3.createBucket(bucketRequest);
            var bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName + " is ready");
            return bucketName;
        } catch (AwsServiceException e) {
            System.err.println("bucket creation " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public void deleteBucket(String bucketName) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while (listObjectsV2Response.isTruncated());

            deleteEmptyBucket(bucketName);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void deleteEmptyBucket(String bucketName) {
        var deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("Successfully deleted " + bucketName);
        } catch (AwsServiceException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void deleteAllBuckets() {
        deleteBuckets(getAllBuckets());
    }

    public List<Bucket> getAllBuckets() {
        var listBucketsRequest = ListBucketsRequest.builder().build();
        var listBucketsResponse = s3.listBuckets(listBucketsRequest);
        return listBucketsResponse.buckets();
    }

    public void deleteBuckets(List<Bucket> buckets) {
        if (buckets.isEmpty())
            return;

        buckets.stream()
                .map(Bucket::name)
                .forEach(this::deleteBucket);

    }

    public byte[] getObjectBytes(String bucketName, String keyName) {
        try {
            // create a GetObjectRequest instance
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            // get the byte[] from this AWS S3 object
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            System.out.println("Successfully got file " + keyName);
            return objectBytes.asByteArray();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void saveFile(String bucketName, String keyName, String newFileName) {
        File file = new File(newFileName);
        file.delete();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        try {
            s3.getObject(getObjectRequest, ResponseTransformer.toFile(file));
            System.out.printf(
                    "Successfully downloaded file '%s' to '%s' file\n",
                    keyName, newFileName);

        } catch (AwsServiceException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public ResponseInputStream<GetObjectResponse> getObject(String bucketName, String keyName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        try {
            ResponseInputStream<GetObjectResponse> object = s3.getObject(getObjectRequest);
            System.out.printf(
                    "Successfully got object '%s'\n",
                    keyName);
            return object;
        } catch (AwsServiceException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    // Returns the names of all images in the given bucket
    public List<String> ListBucketObjects(String bucketName) {
        String keyName;

        var keys = new ArrayList<String>();

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (S3Object myValue : objects) {
                keyName = myValue.key();
                keys.add(keyName);
            }

            return keys;

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    // Places an image into a S3 bucket
    public String putFile(byte[] data, String bucketName, String objectKey) {
        try {
            //Put a file into the bucket
            PutObjectResponse response = s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .build(),
                    RequestBody.fromBytes(data));

            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.err.println(e.awsErrorDetails());
            System.exit(1);
        }
        return "";
    }

    // Places a file into a S3 bucket
    public String putFile(File file, String bucketName, String objectKey) {
        try {
            //Put a file into the bucket
            PutObjectResponse response = s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .build(),
                    RequestBody.fromFile(file));
            System.out.printf(
                    "Successfully uploaded file %s to %s bucket\n",
                    objectKey, bucketName);
            return response.eTag();

        } catch (AwsServiceException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    public URL getUrl(String bucketName, String key) {
        var utilities = s3.utilities();
        var request = GetUrlRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return utilities.getUrl(request);
    }
}