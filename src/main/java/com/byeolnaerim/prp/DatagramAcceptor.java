package com.byeolnaerim.prp;

import java.util.concurrent.CompletionStage;

/** Intercepts selected native PRP datagrams before unmatched datagrams reach ReactiveSession.datagrams(). */
public interface DatagramAcceptor {
    boolean accepts(ReactiveSession session, SessionDatagram datagram);
    CompletionStage<Void> handle(ReactiveSession session, SessionDatagram datagram);
}
