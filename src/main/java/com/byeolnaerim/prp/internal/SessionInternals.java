package com.byeolnaerim.prp.internal;

import com.byeolnaerim.prp.SignalAcceptor;
import com.byeolnaerim.prp.DatagramAcceptor;
import com.byeolnaerim.prp.StreamAcceptor;
import com.byeolnaerim.prp.core.CapabilitySet;

public interface SessionInternals {
    AutoCloseable addStreamAcceptor(StreamAcceptor acceptor);
    AutoCloseable addSignalAcceptor(SignalAcceptor acceptor);
    AutoCloseable addDatagramAcceptor(DatagramAcceptor acceptor);
    CapabilitySet capabilities();
    <T> void attachment(Class<T> type, T value);
    <T> T attachment(Class<T> type);
}
