package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
/**
 * It indicates the state of the remote log segment. This will be based on the action executed on this
 * segment by the remote log service implementation.
 * <p>
 * It goes through the below state transitions.
 * <p>
 * <pre>
 * +---------------------+            +----------------------+
 * |COPY_SEGMENT_STARTED |----------->|COPY_SEGMENT_FINISHED |
 * +-------------------+-+            +--+-------------------+
 *                     |                 |
 *                     |                 |
 *                     v                 v
 *                  +--+-----------------+-+
 *                  |DELETE_SEGMENT_STARTED|
 *                  +-----------+----------+
 *                              |
 *                              |
 *                              v
 *                  +-----------+-----------+
 *                  |DELETE_SEGMENT_FINISHED|
 *                  +-----------------------+
 * </pre>
 */
@InterfaceStability.Evolving
public enum RemoteLogSegmentState {

    /**
     * This state indicates that the segment copying to remote storage is started but not yet finished.
     */
    COPY_SEGMENT_STARTED((byte) 0),

    /**
     * This state indicates that the segment copying to remote storage is finished.
     */
    COPY_SEGMENT_FINISHED((byte) 1),

    /**
     * This state indicates that the segment deletion is started but not yet finished.
     */
    DELETE_SEGMENT_STARTED((byte) 2),

    /**
     * This state indicates that the segment is deleted successfully.
     */
    DELETE_SEGMENT_FINISHED((byte) 3);

    private static final Map<Byte, RemoteLogSegmentState> STATE_TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(RemoteLogSegmentState::id, Function.identity())));

    private final byte id;

    RemoteLogSegmentState(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static RemoteLogSegmentState forId(byte id) {
        return STATE_TYPES.get(id);
    }
}
