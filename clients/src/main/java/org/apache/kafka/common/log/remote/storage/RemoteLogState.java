package org.apache.kafka.common.log.remote.storage;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * It indicates the state of the remote log segment or partition. This will be based on the action executed on this
 * segment or partition by the remote log service implementation.
 * <p>
 * todo: check whether the state validations to be checked or not, add next possible states for each state.
 */
public enum RemoteLogState {

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
    DELETE_SEGMENT_FINISHED((byte) 3),

    /**
     * This is used when a topic/partition is deleted by controller.
     * This partition is marked for delete by controller. That means, all its remote log segments are eligible for
     * deletion so that remote log cleaners can start deleting them.
     */
    DELETE_PARTITION_MARKED((byte) 4),

    /**
     * This state indicates that the partition deletion is started but not yet finished.
     */
    DELETE_PARTITION_STARTED((byte) 5),

    /**
     * This state indicates that the partition is deleted successfully.
     */
    DELETE_PARTITION_FINISHED((byte) 6);

    private static final Map<Byte, RemoteLogState> STATE_TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(RemoteLogState::id, Function.identity())));

    private final byte id;

    RemoteLogState(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static RemoteLogState forId(byte id) {
        return STATE_TYPES.get(id);
    }
}
