package services;

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
    String jobFlowId;
    String flowName;

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

    public void addStep(String stepName, String jar, String mainClass, String... args) {
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

    public String runFlow(int instanceCount, String name, String logUri) {
        // Run the flow on EMR
        flowName = name;

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
        jobFlowId = runJobFlowResponse.jobFlowId();
        System.out.println("Submitted EMR job: %s with the following id: %d" + flowName + jobFlowId);
        System.out.println("Running the following steps:");
        steps.forEach(step -> System.out.println(step.name() + '\n'));
        jobFlowId = runJobFlowResponse.jobFlowId();
        return waitForCompletion();

    }

    public String waitForCompletion() {
        // Wait for the flow to complete
//        TODO: cleanup
        System.out.println("Waiting for the job flow to complete: %s with the following id: %d" + flowName + jobFlowId);
        while (true) {

            ClusterState state = ClusterState.STARTING;
            ClusterStatus status = getClusterStatus(jobFlowId);
            ClusterState newState = status.state();
            if (!state.equals(newState)) {
                System.out.println("Cluster id %s switched from %s to %s. Reason: %s." +
                        jobFlowId + state + newState + status.stateChangeReason());
                state = newState;
            }
            if (state.equals(ClusterState.TERMINATED)) {
                System.out.println("Job flow completed successfully");
                return jobFlowId;
            }
            if (state.equals(ClusterState.TERMINATED_WITH_ERRORS)) {
                System.out.println("Job flow completed with errors");
                return null;
            }
            switch (state.toString()) {
                case "TERMINATED":
                    System.out.println("Job flow completed successfully");
                    return jobFlowId;
                case "TERMINATED_WITH_ERRORS":
                    System.out.println("Job flow completed with errors");
                    return null;
                default:
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public ClusterState getJobState(String jobFlowId) {
        return getClusterStatus(jobFlowId).state();
    }

    public ClusterStatus getClusterStatus(String jobFlowId) {
        DescribeClusterRequest clusterRequest =
                DescribeClusterRequest.builder().clusterId(jobFlowId).build();
        DescribeClusterResponse clusterResponse = emr.describeCluster(clusterRequest);
        return clusterResponse.cluster().status();
    }

}
