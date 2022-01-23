/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.async.PageFrameDispatchTask;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.mp.*;
import io.questdb.std.Misc;
import io.questdb.tasks.*;

import java.io.Closeable;

public interface MessageBus extends Closeable {
    @Override
    default void close() {
        Misc.free(getO3PartitionQueue());
        Misc.free(getTableWriterCommandQueue());
        Misc.free(getTableWriterEventQueue());
        for (int i = 0, n = getPageFrameReduceShardCount(); i < n; i++) {
            Misc.free(getPageFrameReduceQueue(i));
        }
    }

    CairoConfiguration getConfiguration();

    MPSequence getPageFrameDispatchPubSeq();

    RingQueue<PageFrameDispatchTask> getPageFrameDispatchQueue();

    MCSequence getPageFrameDispatchSubSeq();

    Sequence getIndexerPubSequence();

    RingQueue<ColumnIndexerTask> getIndexerQueue();

    Sequence getIndexerSubSequence();

    Sequence getLatestByPubSeq();

    RingQueue<LatestByTask> getLatestByQueue();

    Sequence getLatestBySubSeq();

    MPSequence getO3CallbackPubSeq();

    RingQueue<O3CallbackTask> getO3CallbackQueue();

    MCSequence getO3CallbackSubSeq();

    MPSequence getO3CopyPubSeq();

    RingQueue<O3CopyTask> getO3CopyQueue();

    MCSequence getO3CopySubSeq();

    MPSequence getO3OpenColumnPubSeq();

    RingQueue<O3OpenColumnTask> getO3OpenColumnQueue();

    MCSequence getO3OpenColumnSubSeq();

    MPSequence getO3PartitionPubSeq();

    RingQueue<O3PartitionTask> getO3PartitionQueue();

    MCSequence getO3PartitionSubSeq();

    MPSequence getO3PurgeDiscoveryPubSeq();

    RingQueue<O3PurgeDiscoveryTask> getO3PurgeDiscoveryQueue();

    MCSequence getO3PurgeDiscoverySubSeq();

    FanOut getPageFrameCollectFanOut(int shard);

    MPSequence getPageFrameReducePubSeq(int shard);

    RingQueue<PageFrameReduceTask> getPageFrameReduceQueue(int shard);

    int getPageFrameReduceShardCount();

    MCSequence getPageFrameReduceSubSeq(int shard);

    MCSequence getPageFrameCleanupSubSeq(int shard);

    FanOut getTableWriterCommandFanOut();

    MPSequence getTableWriterCommandPubSeq();

    RingQueue<TableWriterTask> getTableWriterCommandQueue();

    FanOut getTableWriterEventFanOut();

    MPSequence getTableWriterEventPubSeq();

    RingQueue<TableWriterTask> getTableWriterEventQueue();

    Sequence getVectorAggregatePubSeq();

    RingQueue<VectorAggregateTask> getVectorAggregateQueue();

    Sequence getVectorAggregateSubSeq();
}
