/*
 * Copyright 2021 Tobias Schumacher
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package de.tschumacher.queueservice.sqs;

import com.amazonaws.regions.Regions;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class SQSQueueConfiguration {
    @NonNull
    private String secretKey;

    @NonNull
    private String accessKey;

    @NonNull
    private String queueName;

    @Builder.Default
    private String defaultRegion = Regions.EU_CENTRAL_1.getName();

    @Builder.Default
    private int visibilityTimeout = 60 * 5;

    @Builder.Default
    private int waitTimeSeconds = 20;

    @Builder.Default
    private int maxNumberOfMessages = 1;

    @Builder.Default
    private int retrySeconds = 60 * 2;

    public boolean isFifo() {
        return queueName.toLowerCase().endsWith(".fifo");
    }
}
