/*
 * Copyright 2017 StreamSets Inc.
 *
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
package epam.stage.processor.sample;

import ch.hsr.geohash.GeoHash;
import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import epam.stage.lib.sample.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public abstract class SampleProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SampleProcessor.class);

    public abstract String getConfig();

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        if (getConfig().equals("invalidValue")) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.SAMPLE.name(), "config", Errors.SAMPLE_00, "Here's what's wrong..."
                    )
            );
        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        String address = record.get("/Address").getValueAsString();
        String city = record.get("/City").getValueAsString();
        String country = record.get("/Country").getValueAsString();
        String request = String.format("%s, %s, %s", address, city, country);
        JOpenCageLatLng resultLatLng = this.getRecord(request);

        try {
            record.set("/Latitude", Field.create(String.format("%.3f", resultLatLng.getLat())));
            record.set("/Longitude", Field.create(String.format("%.3f", resultLatLng.getLng())));
            batchMaker.addRecord(record);
        } catch (Exception var9) {
            throw new OnRecordErrorException(record, Errors.SAMPLE_00, new Object[]{var9});
        }
    }

    private JOpenCageLatLng getRecord(String request) {
        JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder("");
        JOpenCageForwardRequest jOpenRequest = new JOpenCageForwardRequest(request);
        JOpenCageResponse response = jOpenCageGeocoder.forward(jOpenRequest);
        return response.getFirstPosition();
    }
}
