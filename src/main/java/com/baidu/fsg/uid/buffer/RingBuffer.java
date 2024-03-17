/*
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserve.
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
package com.baidu.fsg.uid.buffer;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.baidu.fsg.uid.utils.PaddedAtomicLong;

/**
 * Represents a ring buffer based on array.<br>
 * Using array could improve read element performance due to the CUP cache line. To prevent 
 * the side effect of False Sharing, {@link PaddedAtomicLong} is using on 'tail' and 'cursor'<p>
 * 
 * A ring buffer is consisted of:
 * <li><b>slots:</b> each element of the array is a slot, which is be set with a UID
 * <li><b>flags:</b> flag array corresponding the same index with the slots, indicates whether can take or put slot
 * <li><b>tail:</b> a sequence of the max slot position to produce 
 * <li><b>cursor:</b> a sequence of the min slot position to consume
 * 
 * @author yutianbao
 */

// 环形缓冲区的实现
public class RingBuffer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RingBuffer.class);

    /** Constants */
    private static final int START_POINT = -1;

    // 对应下面的flags的状态，是可填充的状态
    private static final long CAN_PUT_FLAG = 0L;
    // 对应下面flags的状态，是可获取的状态
    private static final long CAN_TAKE_FLAG = 1L;
    public static final int DEFAULT_PADDING_PERCENT = 50;

    /** The size of RingBuffer's slots, each slot hold a UID */
    // 环形缓冲区的长度
    private final int bufferSize;
    private final long indexMask;
    // 环形缓冲区内部实际是一个数组，用于存放id
    private final long[] slots;
    // 这是标记位，slot的状态的
    private final PaddedAtomicLong[] flags;

    /** Tail: last position sequence to produce */
    // tail 指针，他走过的地方代表生产过的地方
    private final AtomicLong tail = new PaddedAtomicLong(START_POINT);

    /** Cursor: current position sequence to consume */
    // cursor指针， 他走过的地方代表消费的地方
    private final AtomicLong cursor = new PaddedAtomicLong(START_POINT);
    // 填充阈值，不过不是百分比，而是真实的索引位置
    /** Threshold for trigger padding buffer*/
    private final int paddingThreshold; 
    
    /** Reject put/take buffer handle policy */
    private RejectedPutBufferHandler rejectedPutHandler = this::discardPutBuffer;
    private RejectedTakeBufferHandler rejectedTakeHandler = this::exceptionRejectedTakeBuffer; 
    
    /** Executor of padding buffer */
    private BufferPaddingExecutor bufferPaddingExecutor;

    /**
     * Constructor with buffer size, paddingFactor default as {@value #DEFAULT_PADDING_PERCENT}
     * 
     * @param bufferSize must be positive & a power of 2
     */
    public RingBuffer(int bufferSize) {
        this(bufferSize, DEFAULT_PADDING_PERCENT);
    }
    
    /**
     * Constructor with buffer size & padding factor
     * 
     * @param bufferSize must be positive & a power of 2
     * @param paddingFactor percent in (0 - 100). When the count of rest available UIDs reach the threshold, it will trigger padding buffer<br>
     *        Sample: paddingFactor=20, bufferSize=1000 -> threshold=1000 * 20 /100,  
     *        padding buffer will be triggered when tail-cursor<threshold
     */
    // paddingFactor 填充阈值，如果剩余的可用id数量的百分比，小于这个值的话，就开始重新填充，这是个百分比
    public RingBuffer(int bufferSize, int paddingFactor) {
        // check buffer size is positive & a power of 2; padding factor in (0, 100)
        Assert.isTrue(bufferSize > 0L, "RingBuffer size must be positive");
        Assert.isTrue(Integer.bitCount(bufferSize) == 1, "RingBuffer size must be a power of 2");
        Assert.isTrue(paddingFactor > 0 && paddingFactor < 100, "RingBuffer size must be positive");

        this.bufferSize = bufferSize;
        this.indexMask = bufferSize - 1;
        this.slots = new long[bufferSize];
        this.flags = initFlags(bufferSize);

        // 计算百分比对应的真正的阈值位置
        this.paddingThreshold = bufferSize * paddingFactor / 100;
    }

    /**
     * Put an UID in the ring & tail moved<br>
     * We use 'synchronized' to guarantee the UID fill in slot & publish new tail sequence as atomic operations<br>
     * 
     * <b>Note that: </b> It is recommended to put UID in a serialize way, cause we once batch generate a series UIDs and put
     * the one by one into the buffer, so it is unnecessary put in multi-threads
     *
     * @param uid
     * @return false means that the buffer is full, apply {@link RejectedPutBufferHandler}
     */
    // 往环形数组中填充一个uid值
    public synchronized boolean put(long uid) {
        long currentTail = tail.get();
        long currentCursor = cursor.get();

        // tail catches the cursor, means that you can't put any cause of RingBuffer is full
        // 他先计算当前两个指针的相隔距离
        long distance = currentTail - (currentCursor == START_POINT ? 0 : currentCursor);
        // 如果距离为缓冲区的长度，生产的数量已经到顶了，不能再生产了，走拒绝put策略
        if (distance == bufferSize - 1) {
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 检查当前尾部+1的位置是不是能进行放入数据
        // 也就是从flags中获取状态
        // 1. pre-check whether the flag is CAN_PUT_FLAG
        int nextTailIndex = calSlotIndex(currentTail + 1);
        // 如果不可以，走拒绝put策略
        if (flags[nextTailIndex].get() != CAN_PUT_FLAG) {
            rejectedPutHandler.rejectPutBuffer(this, uid);
            return false;
        }

        // 校验通过，让如uid到slot ，设置flags，并移动尾指针
        // 2. put UID in the next slot
        // 3. update next slot' flag to CAN_TAKE_FLAG
        // 4. publish tail with sequence increase by one
        slots[nextTailIndex] = uid;
        flags[nextTailIndex].set(CAN_TAKE_FLAG);
        tail.incrementAndGet();

        // The atomicity of operations above, guarantees by 'synchronized'. In another word,
        // the take operation can't consume the UID we just put, until the tail is published(tail.incrementAndGet())
        return true;
    }

    /**
     * Take an UID of the ring at the next cursor, this is a lock free operation by using atomic cursor<p>
     * 
     * Before getting the UID, we also check whether reach the padding threshold, 
     * the padding buffer operation will be triggered in another thread<br>
     * If there is no more available UID to be taken, the specified {@link RejectedTakeBufferHandler} will be applied<br>
     * 
     * @return UID
     * @throws IllegalStateException if the cursor moved back
     */
    // 从环形缓冲区中获取一个元素
    public long take() {
        // spin get next available cursor
        long currentCursor = cursor.get();
        long nextCursor = cursor.updateAndGet(old -> old == tail.get() ? old : old + 1);

        // 如果生产消费指针大于等于生产指针，说明没得消费了
        // check for safety consideration, it never occurs
        Assert.isTrue(nextCursor >= currentCursor, "Curosr can't move back");

        // 如果到达扩容的阈值，执行对其操作
        // trigger padding in an async-mode if reach the threshold
        long currentTail = tail.get();
        if (currentTail - nextCursor < paddingThreshold) {
            LOGGER.info("Reach the padding threshold:{}. tail:{}, cursor:{}, rest:{}", paddingThreshold, currentTail,
                    nextCursor, currentTail - nextCursor);
            // 对其操作是使用一个cpu核心数*2的固定数量的线程池
            bufferPaddingExecutor.asyncPadding();
        }

        // 如果两个指针相遇，说明数组已经满了
        // cursor catch the tail, means that there is no more available UID to take
        if (nextCursor == currentCursor) {
            rejectedTakeHandler.rejectTakeBuffer(this);
        }

        // 检查是否可以当前位置获取uid
        // 1. check next slot flag is CAN_TAKE_FLAG
        int nextCursorIndex = calSlotIndex(nextCursor);
        Assert.isTrue(flags[nextCursorIndex].get() == CAN_TAKE_FLAG, "Curosr not in can take status");

        // 2. get UID from next slot
        // 3. set next slot flag as CAN_PUT_FLAG.
        // 获取uid，并设置slot位为可填充状态
        long uid = slots[nextCursorIndex];
        flags[nextCursorIndex].set(CAN_PUT_FLAG);

        // Note that: Step 2,3 can not swap. If we set flag before get value of slot, the producer may overwrite the
        // slot with a new UID, and this may cause the consumer take the UID twice after walk a round the ring
        return uid;
    }

    /**
     * Calculate slot index with the slot sequence (sequence % bufferSize) 
     */
    protected int calSlotIndex(long sequence) {
        return (int) (sequence & indexMask);
    }

    /**
     * Discard policy for {@link RejectedPutBufferHandler}, we just do logging
     */
    protected void discardPutBuffer(RingBuffer ringBuffer, long uid) {
        LOGGER.warn("Rejected putting buffer for uid:{}. {}", uid, ringBuffer);
    }
    
    /**
     * Policy for {@link RejectedTakeBufferHandler}, throws {@link RuntimeException} after logging 
     */
    protected void exceptionRejectedTakeBuffer(RingBuffer ringBuffer) {
        LOGGER.warn("Rejected take buffer. {}", ringBuffer);
        throw new RuntimeException("Rejected take buffer. " + ringBuffer);
    }
    
    /**
     * Initialize flags as CAN_PUT_FLAG
     */
    private PaddedAtomicLong[] initFlags(int bufferSize) {
        PaddedAtomicLong[] flags = new PaddedAtomicLong[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            flags[i] = new PaddedAtomicLong(CAN_PUT_FLAG);
        }
        
        return flags;
    }

    /**
     * Getters
     */
    public long getTail() {
        return tail.get();
    }

    public long getCursor() {
        return cursor.get();
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Setters
     */
    public void setBufferPaddingExecutor(BufferPaddingExecutor bufferPaddingExecutor) {
        this.bufferPaddingExecutor = bufferPaddingExecutor;
    }

    public void setRejectedPutHandler(RejectedPutBufferHandler rejectedPutHandler) {
        this.rejectedPutHandler = rejectedPutHandler;
    }

    public void setRejectedTakeHandler(RejectedTakeBufferHandler rejectedTakeHandler) {
        this.rejectedTakeHandler = rejectedTakeHandler;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RingBuffer [bufferSize=").append(bufferSize)
               .append(", tail=").append(tail)
               .append(", cursor=").append(cursor)
               .append(", paddingThreshold=").append(paddingThreshold).append("]");
        
        return builder.toString();
    }

}
