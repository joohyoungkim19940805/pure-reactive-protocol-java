package com.byeolnaerim.prp.transport;

import java.util.concurrent.CompletionStage;

public interface TransportConnection {
    TransportDescription description();
    default boolean supportsLane(LaneRequirements requirements) {
        return "reliable".equals(requirements.reliability()) && "ordered".equals(requirements.ordering());
    }
    CompletionStage<TransportLane> openLane(LaneRequirements requirements);
    CompletionStage<TransportCloseEvent> closed();
    CompletionStage<Void> close(Integer code, String reason);
}
