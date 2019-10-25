package com.mamba.arch.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Snowflake {

    private static final Logger LOGGER = LoggerFactory.getLogger(Snowflake.class);

    private static final int SEQUENCE_MAX = 4095; //12bit

    private static final int SEQUENCE_SHIFT = 12;

    private static final int TIME_SHIFT = 22;

    private final long node;  //10bit

    private final long offset;  //time offset

    private volatile long sequence;

    public Snowflake(int node) {
        this(node, 1546300800000L); //2019-01-01 00:00:00.000 +0000
    }

    public Snowflake(int node, long timeOffset) {
        if (node < 0 || node > 1023) {
            throw new IllegalArgumentException("node is out of range[0, 1023]! node=" + node);
        }
        if (timeOffset < 0) {
            throw new IllegalArgumentException("time offset can not be less than 0! offset=" + timeOffset);
        }
        long initTime = System.currentTimeMillis();
        if (timeOffset > initTime) {
            throw new IllegalArgumentException("time offset can not be great than current time in millis! offset=" + timeOffset);
        }
        this.node = ((long) node) << SEQUENCE_SHIFT;
        this.offset = timeOffset << TIME_SHIFT;
        this.sequence = initTime << TIME_SHIFT;
        LOGGER.info("snowflake init. node: {}, time offset: {}, init timestamp: {}", node, timeOffset, initTime);
    }

    public long next() {
        long sequence = this.sequence(System.currentTimeMillis());
        //TODO: 更新当前reference time到本地，防止时钟回拨后应用重启
        return (sequence - this.offset) | this.node | (sequence & SEQUENCE_MAX);
    }

    private synchronized long sequence(long currentTime) {
        long referenceTime = (this.sequence >> TIME_SHIFT);
        if (currentTime > referenceTime) {
            this.sequence = (currentTime << TIME_SHIFT) + (currentTime & 0xf); //末尾随机
        } else {
            if (currentTime < referenceTime) {//当前时间小于reference时间，可能出现时钟回拨。
                LOGGER.warn("Last referenceTime {} is after currentTime {}", referenceTime, currentTime);
            }
            if ((this.sequence & SEQUENCE_MAX) == SEQUENCE_MAX) {
                //解决同一时间Sequence超出问题，referenceTime + 1，警告提醒
                LOGGER.warn("Sequence exhausted at 4096");
                this.sequence = (referenceTime << TIME_SHIFT);
            } else {
                this.sequence++;
            }
        }
        return this.sequence;
    }
}
