/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.api.table.catalog.TablePath;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class JobDAGInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    Long jobId;
    Map<String, Object> envOptions;
    Map<Integer, List<Edge>> pipelineEdges;
    Map<Long, VertexInfo> vertexInfoMap;
    ExecutionAddress master;
    Set<ExecutionAddress> historyExecutionPlan;

    public JsonObject toJsonObject() {
        JsonObject pipelineEdgesJsonObject = new JsonObject();

        if (pipelineEdges != null) {
            for (Map.Entry<Integer, List<Edge>> entry : pipelineEdges.entrySet()) {
                // Issue 3 Fix: Skip null pipeline keys entirely to protect UI JSON contract
                if (entry.getKey() == null) {
                    continue;
                }

                JsonArray jsonArray = new JsonArray();
                if (entry.getValue() != null) {
                    for (Edge edge : entry.getValue()) {
                        if (edge != null) {
                            // Issue 1 Fix: Null-check the wrapper IDs before calling .toString()
                            if (edge.getInputVertexId() == null || edge.getTargetVertexId() == null) {
                                continue; 
                            }
                            JsonObject edgeJsonObject = new JsonObject();
                            edgeJsonObject.add("inputVertexId", edge.getInputVertexId().toString());
                            edgeJsonObject.add("targetVertexId", edge.getTargetVertexId().toString());
                            jsonArray.add(edgeJsonObject);
                        }
                    }
                }
                pipelineEdgesJsonObject.add(entry.getKey().toString(), jsonArray);
            }
        }

        JsonObject jsonObject = new JsonObject();
        // Issue 2 Fix: Safe string conversion for jobId
        jsonObject.add("jobId", jobId != null ? jobId.toString() : "");
        jsonObject.add("pipelineEdges", pipelineEdgesJsonObject);
        
        // Issue 2 Fix: Prevent JsonUtil from crashing on null envOptions
        if (envOptions != null) {
            jsonObject.add("envOptions", JsonUtil.toJsonObject(envOptions));
        } else {
            jsonObject.add("envOptions", new JsonObject());
        }

        JsonArray vertexInfoMapString = new JsonArray();
        if (vertexInfoMap != null) {
            for (Map.Entry<Long, VertexInfo> entry : vertexInfoMap.entrySet()) {
                VertexInfo vertexInfo = entry.getValue();
                if (vertexInfo == null) {
                    continue;
                }
                
                JsonObject vertexInfoJsonObj = new JsonObject();
                vertexInfoJsonObj.add("vertexId", vertexInfo.getVertexId() != null ? vertexInfo.getVertexId().toString() : "");
                String typeStr = (vertexInfo.getType() != null && vertexInfo.getType().getType() != null) 
                        ? vertexInfo.getType().getType() 
                        : "";
                vertexInfoJsonObj.add("type", typeStr);
                
                vertexInfoJsonObj.add("vertexName", vertexInfo.getConnectorType() != null ? vertexInfo.getConnectorType() : "");
                
                JsonArray tablePaths = new JsonArray();
                if (vertexInfo.getTablePaths() != null) {
                    for (TablePath tablePath : vertexInfo.getTablePaths()) {
                        if (tablePath != null) {
                            tablePaths.add(tablePath.toString());
                        }
                    }
                }
                vertexInfoJsonObj.add("tablePaths", tablePaths);
                vertexInfoMapString.add(vertexInfoJsonObj);
            }
        }
        jsonObject.add("vertexInfoMap", vertexInfoMapString);
        return jsonObject;
    }
