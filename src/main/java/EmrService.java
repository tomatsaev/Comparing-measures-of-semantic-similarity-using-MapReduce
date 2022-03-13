import lombok.Builder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;

@Builder
public class EmrService {

    @Builder.Default
    EmrClient emr = getClient();
    @Builder.Default
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    public static EmrService create() {
        return builder().build();
    }

    private static EmrClient getClient() {
        // Create the EmrClient object
        Region region = Region.US_EAST_1;
        return EmrClient.builder()
                .region(region)
                .build();
    }

}
