// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.binding;

import com.google.protobuf.InvalidProtocolBufferException;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class JniSinkWriterStreamRequest implements AutoCloseable {
    private final ConnectorServiceProto.SinkWriterStreamRequest pbRequest;
    private final StreamChunk chunk;
    private final long epoch;
    private final long batchId;
    private final boolean isPb;

    JniSinkWriterStreamRequest(ConnectorServiceProto.SinkWriterStreamRequest pbRequest) {
        this.pbRequest = pbRequest;
        this.chunk = null;
        this.epoch = 0;
        this.batchId = 0;
        this.isPb = true;
    }

    JniSinkWriterStreamRequest(StreamChunk chunk, long epoch, long batchId) {
        this.pbRequest = null;
        this.chunk = chunk;
        this.epoch = epoch;
        this.batchId = batchId;
        this.isPb = false;
    }

    public static JniSinkWriterStreamRequest fromSerializedPayload(byte[] payload) {
        try {
            return new JniSinkWriterStreamRequest(
                    ConnectorServiceProto.SinkWriterStreamRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static JniSinkWriterStreamRequest fromStreamChunkOwnedPointer(
            long[] arrowArrayPointers,
            long[] arrowSchemaPointers,
            long epoch,
            long batchId,
            long pointer) {
        return new JniSinkWriterStreamRequest(
                new StreamChunk(
                        Arrays.stream(arrowArrayPointers)
                                .boxed()
                                .collect(Collectors.toCollection(ArrayList::new)),
                        Arrays.stream(arrowSchemaPointers)
                                .boxed()
                                .collect(Collectors.toCollection(ArrayList::new))),
                epoch,
                batchId);
    }

    public ConnectorServiceProto.SinkWriterStreamRequest asPbRequest() {
        if (isPb) {
            return pbRequest;
        } else {
            return ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                    .setWriteBatch(
                            ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch.newBuilder()
                                    .setEpoch(epoch)
                                    .setBatchId(batchId)
                                    .setStreamChunkRefPointer(chunk.getPointer())
                                    .addAllArrowArrayPointers(chunk.getArrowArrayPointers())
                                    .addAllArrowSchemaPointers(chunk.getArrowSchemaPointers())
                                    .build())
                    .build();
        }
    }

    @Override
    public void close() throws Exception {
        if (!isPb && chunk != null) {
            this.chunk.close();
        }
    }
}

// class ConverterJni {
//     native public static void fill_arr(long arrAddr, long schemaAddr);
// }
