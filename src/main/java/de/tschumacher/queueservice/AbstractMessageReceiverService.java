/*
 * Copyright 2015 Tobias Schumacher
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
package de.tschumacher.queueservice;

import de.tschumacher.queueservice.sqs.SQSQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessageReceiverService<F> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageReceiverService.class);
    private static final int WORKER_COUNT = 5;

    private final ExecutorService executorService;
    private boolean running = false;
    private final Runnable worker;
    private final MessageReceiver<F> messageReceiver;

    public AbstractMessageReceiverService(final SQSQueue queue, MessageReceiver<F> messageReceiver) {
        super();
        this.messageReceiver = messageReceiver;
        this.worker = newWorker(queue);
        this.executorService = Executors.newFixedThreadPool(WORKER_COUNT);
    }

    public void start() {
        this.running = true;
        for (int i = 0; i < WORKER_COUNT; i++) {
            this.executorService.submit(this.worker);
        }
    }

    public void stop() throws InterruptedException {
        this.running = false;
        this.executorService.shutdown();
    }

    private Runnable newWorker(final SQSQueue queue) {
        return () -> {
            while (AbstractMessageReceiverService.this.running) {
                try {
                    AbstractMessageReceiverService.this.messageReceiver.receiveMessage(queue);
                } catch (final Throwable e) {
                    logger.error("could not handle message", e);
                }
            }
        };
    }

    @Override
    protected void finalize() throws Throwable {
        stop();
        super.finalize();
    }
}
