/*
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Int64Value;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import io.grpc.ManagedChannelBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import org.tensorflow.framework.DataType;
import org.tensorflow.framework.TensorProto;
import org.tensorflow.framework.TensorProto.Builder;
import org.tensorflow.framework.TensorShapeProto;
import org.tensorflow.framework.TensorShapeProto.Dim;
import support.WordIndex;
import tensorflow.serving.Model.ModelSpec;
import tensorflow.serving.Predict.PredictRequest;
import tensorflow.serving.Predict.PredictResponse;
import tensorflow.serving.PredictionServiceGrpc;

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;

/**
 * Shows how to enrich a stream of movie reviews with classification using
 * a pre-trained TensorFlow model. Executes the TensorFlow model using gRPC
 * calls to a TensorFlow Model Server.
 */
public class ModelServerClassification {

    private static Pipeline buildPipeline(String serverAddress, IMap<Long, String> reviewsMap) {

        // Service used to convert the review into a prediction request.
        final ServiceFactory<WordIndex, WordIndex> wordIndexService = ServiceFactory
                .withCreateContextFn(context -> {
                    WordIndex wordIndex = new WordIndex(context.attachedDirectory("data"));
                    return wordIndex;
                })
                .withCreateServiceFn((ctx, wordIndex) -> wordIndex);

        // GRPC service that calls the Model server.
        final ServiceFactory<?, ? extends GrpcService<PredictRequest, PredictResponse>> predictionService = unaryService(
                () -> ManagedChannelBuilder.forTarget(serverAddress).usePlaintext(),
                channel -> PredictionServiceGrpc.newStub(channel)::predict
        );

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(reviewsMap))
         .map(Entry::getValue)
         .mapUsingService(wordIndexService, (service, review) -> {
             float[][] featuresTensorData = service.createTensorInput(review);
             Builder featuresTensorBuilder = TensorProto.newBuilder();
             for (float[] featuresTensorDatum : featuresTensorData) {
                 for (float v : featuresTensorDatum) {
                     featuresTensorBuilder.addFloatVal(v);
                 }
             }
             Dim featuresDim1 =
                     Dim.newBuilder().setSize(featuresTensorData.length).build();
             Dim featuresDim2 =
                     Dim.newBuilder().setSize(featuresTensorData[0].length).build();
             TensorShapeProto featuresShape =
                     TensorShapeProto.newBuilder().addDim(featuresDim1).addDim(featuresDim2).build();
             featuresTensorBuilder.setDtype(DataType.DT_FLOAT)
                                  .setTensorShape(featuresShape);
             TensorProto featuresTensorProto = featuresTensorBuilder.build();

             // Generate gRPC request
             Int64Value version = Int64Value.newBuilder().setValue(1).build();
             ModelSpec modelSpec =
                     ModelSpec.newBuilder().setName("reviewSentiment").setVersion(version).build();
             PredictRequest request = PredictRequest.newBuilder()
                                                    .setModelSpec(modelSpec)
                                                    .putInputs("input_review", featuresTensorProto)
                                                    .build();
             return tuple2(review, request);
         })
         .mapUsingServiceAsync(predictionService, (service, tuple2) -> service.call(tuple2.f1()).thenApply(response -> {
             float classification = response
                     .getOutputsOrThrow("dense_1/Sigmoid:0")
                     .getFloatVal(0);
             // emit the review along with the classification
             return tuple2(tuple2.f0(), classification);
         }))
         .setLocalParallelism(1) // one worker is enough to drive they async calls
         .writeTo(Sinks.logger());
        return p;
    }


    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        if (args.length != 2) {
            System.out.println("Usage: ModelServerClassification <data path> <model server address>");
            System.exit(1);
        }
        String dataPath = args[0];
        String serverAddress = args[1];

        JobConfig jobConfig = new JobConfig();
        jobConfig.attachDirectory(dataPath, "data");

        JetInstance instance = Jet.newJetInstance();
        try {
            IMap<Long, String> reviewsMap = instance.getMap("reviewsMap");
            SampleReviews.populateReviewsMap(reviewsMap);

            Pipeline p = buildPipeline(serverAddress, reviewsMap);

            instance.newJob(p, jobConfig).join();
        } finally {
            instance.shutdown();
        }
    }

    /**
     * Adapt a {@link ListenableFuture} to java standard {@link
     * CompletableFuture}, which is used by Jet.
     */
    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> lf) {
        CompletableFuture<T> f = new CompletableFuture<>();
        // note that we don't handle CompletableFuture.cancel()
        Futures.addCallback(lf, new FutureCallback<T>() {
            @Override
            public void onSuccess(@NullableDecl T result) {
                f.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                f.completeExceptionally(t);
            }
        }, directExecutor());
        return f;
    }
}
