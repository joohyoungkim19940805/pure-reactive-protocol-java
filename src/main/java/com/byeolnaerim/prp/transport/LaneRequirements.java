package com.byeolnaerim.prp.transport;

public record LaneRequirements(String reliability, String ordering, int maxFrameBytes) {
    public static LaneRequirements reliableOrdered(int maxFrameBytes) {
        return new LaneRequirements("reliable", "ordered", maxFrameBytes);
    }

    public static LaneRequirements bestEffortUnordered(int maxFrameBytes) {
        return new LaneRequirements("best-effort", "unordered", maxFrameBytes);
    }
}
