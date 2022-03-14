import lombok.Builder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.ArrayList;
import java.util.List;

@Builder
public class EmrService {

    @Builder.Default
    EmrClient emr = getClient();
    @Builder.Default
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    @Builder.Default
    List<StepConfig> steps = new ArrayList<>();
    RunJobFlowResponse runJobFlowResponse;

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

    public void addStep(String stepName, String jar, String mainClass, String ... args) {
        // Add a Hadoop step config to be executed on EMR
        steps.add(
                StepConfig.builder()
                        .name(stepName)
                        .actionOnFailure("TERMINATE_CLUSTER")
                        .hadoopJarStep(
                                HadoopJarStepConfig.builder()
                                        .jar(jar)
                                        .mainClass(mainClass)
                                        .args(args)
                                        .build()
                        )
                        .build()
        );
    }

    public void runFlow(int instanceCount, String flowName, String logUri) {
        // Run the flow on EMR
        RunJobFlowResponse runJobFlowResponse = emr.runJobFlow(
                RunJobFlowRequest.builder()
                        .name(flowName)
                        .logUri(logUri)
                        .steps(steps)
//                        TODO: Purge steps after completion
                        .instances(
                                JobFlowInstancesConfig.builder()
                                        .instanceCount(instanceCount)
                                        .keepJobFlowAliveWhenNoSteps(false)
                                        .hadoopVersion("3.2.1")
                                        .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                                        .masterInstanceType("m4.large")
                                        .slaveInstanceType("m4.large")
                                        .build()
                        )
                        .releaseLabel("emr-6.2.0")
                        .jobFlowRole("EMR_EC2_DefaultRole")
                        .serviceRole("EMR_DefaultRole")
                        .build()
        );
        String jobFlowId = runJobFlowResponse.jobFlowId();
        System.out.println("Running the following job flow: %s with the following id: %d" + flowName + jobFlowId);
        System.out.println("Running the following steps:");
        steps.forEach(step -> System.out.println(step.name() + '\n'));
    }

}
