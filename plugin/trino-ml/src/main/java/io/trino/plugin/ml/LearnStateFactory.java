/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ml;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.airlift.slice.Slice;
import io.trino.array.ObjectBigArray;
import io.trino.array.SliceBigArray;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;
import libsvm.svm_parameter;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;

public class LearnStateFactory
        implements AccumulatorStateFactory<LearnState>
{
    private static final long ARRAY_LIST_SIZE = instanceSize(ArrayList.class);
    private static final long SVM_PARAMETERS_SIZE = instanceSize(svm_parameter.class);

    @Override
    public LearnState createSingleState()
    {
        return new SingleLearnState();
    }

    @Override
    public LearnState createGroupedState()
    {
        return new GroupedLearnState();
    }

    public static class GroupedLearnState
            implements GroupedAccumulatorState, LearnState
    {
        private final ObjectBigArray<List<Double>> labelsArray = new ObjectBigArray<>();
        private final ObjectBigArray<List<FeatureVector>> featureVectorsArray = new ObjectBigArray<>();
        private final SliceBigArray parametersArray = new SliceBigArray();
        private final BiMap<String, Integer> labelEnumeration = HashBiMap.create();
        private int groupId;
        private int nextLabel;
        private long size;

        @Override
        public void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size)
        {
            labelsArray.ensureCapacity(size);
            featureVectorsArray.ensureCapacity(size);
            parametersArray.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + labelsArray.sizeOf() + featureVectorsArray.sizeOf();
        }

        @Override
        public BiMap<String, Integer> getLabelEnumeration()
        {
            return labelEnumeration;
        }

        @Override
        public int enumerateLabel(String label)
        {
            if (!labelEnumeration.containsKey(label)) {
                labelEnumeration.put(label, nextLabel);
                nextLabel++;
            }
            return labelEnumeration.get(label);
        }

        @Override
        public List<Double> getLabels()
        {
            List<Double> labels = labelsArray.get(groupId);
            if (labels == null) {
                labels = new ArrayList<>();
                size += ARRAY_LIST_SIZE;
                // Assume that one parameter will be set for each group of labels
                size += SVM_PARAMETERS_SIZE;
                labelsArray.set(groupId, labels);
            }
            return labels;
        }

        @Override
        public List<FeatureVector> getFeatureVectors()
        {
            List<FeatureVector> featureVectors = featureVectorsArray.get(groupId);
            if (featureVectors == null) {
                featureVectors = new ArrayList<>();
                size += ARRAY_LIST_SIZE;
                featureVectorsArray.set(groupId, featureVectors);
            }
            return featureVectors;
        }

        @Override
        public Slice getParameters()
        {
            return parametersArray.get(groupId);
        }

        @Override
        public void setParameters(Slice parameters)
        {
            parametersArray.set(groupId, parameters);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }
    }

    public static class SingleLearnState
            implements LearnState
    {
        private final List<Double> labels = new ArrayList<>();
        private final List<FeatureVector> featureVectors = new ArrayList<>();
        private final BiMap<String, Integer> labelEnumeration = HashBiMap.create();
        private int nextLabel;
        private Slice parameters;
        private long size;

        @Override
        public long getEstimatedSize()
        {
            return size + 2 * ARRAY_LIST_SIZE;
        }

        @Override
        public BiMap<String, Integer> getLabelEnumeration()
        {
            return labelEnumeration;
        }

        @Override
        public int enumerateLabel(String label)
        {
            if (!labelEnumeration.containsKey(label)) {
                labelEnumeration.put(label, nextLabel);
                nextLabel++;
            }
            return labelEnumeration.get(label);
        }

        @Override
        public List<Double> getLabels()
        {
            return labels;
        }

        @Override
        public List<FeatureVector> getFeatureVectors()
        {
            return featureVectors;
        }

        @Override
        public Slice getParameters()
        {
            return parameters;
        }

        @Override
        public void setParameters(Slice parameters)
        {
            this.parameters = parameters;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }
    }
}
