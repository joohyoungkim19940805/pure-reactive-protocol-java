package com.byeolnaerim.prp.profile.datagram;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface DatagramRouteHandler {
    CompletionStage<Void> handle(RoutedDatagram datagram);
}
