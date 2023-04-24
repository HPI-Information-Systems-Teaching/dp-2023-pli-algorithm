/**
 * Copyright 2014-2016 by Metanome Project
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.metanome.algorithms.plilister;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.BasicStatisticsResultReceiver;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_helper.data_structures.PLIBuilder;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.results.BasicStatistic;
import de.metanome.algorithm_integration.results.basic_statistic_values.BasicStatisticValueIntegerList;
import de.metanome.algorithm_integration.results.basic_statistic_values.BasicStatisticValueString;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class PLILister implements BasicStatisticsAlgorithm, RelationalInputParameterAlgorithm {
    private final String inputName = "INPUT_FILE";
    private RelationalInputGenerator inputGenerator;
    private BasicStatisticsResultReceiver resultReceiver;

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> conf = new ArrayList<>();
        conf.add(new ConfigurationRequirementRelationalInput(inputName));
        return conf;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        RelationalInput input = this.inputGenerator.generateNewCopy();
        PLIBuilder pliBuilder = new PLIBuilder(input);
        List<PositionListIndex> plis = pliBuilder.getPLIList();

        LinkedList<BasicStatistic> results = new LinkedList<>();

        for (int i = 0; i < input.numberOfColumns(); i++) {
            BasicStatistic bs = new BasicStatistic(new ColumnIdentifier(input.relationName(), input.columnNames().get(i)));
            int partitionIdx = 1;
            for (LongArrayList partition : plis.get(i).getClusters()) {
                ArrayList<Integer> p_list = new ArrayList<>();
                for (long l : partition) {
                    p_list.add((int) l);
                }
                bs.addStatistic("partition " + partitionIdx, new BasicStatisticValueIntegerList(p_list));
                partitionIdx++;
            }
            results.add(bs);
        }

        for (BasicStatistic bs : results) {
            this.resultReceiver.receiveResult(bs);
        }
    }

    @Override
    public String getAuthors() {
        return "Youri Kaminsky";
    }

    @Override
    public String getDescription() {
        return "Return the PLIs for all columns in the given dataset.";
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
        if (identifier.equals(inputName)) {
            this.inputGenerator = values[0];
        } else {
            throw new AlgorithmConfigurationException("Unknown input configuration!");
        }
    }

    @Override
    public void setResultReceiver(BasicStatisticsResultReceiver resultReceiver) {
        this.resultReceiver = resultReceiver;
    }
}
