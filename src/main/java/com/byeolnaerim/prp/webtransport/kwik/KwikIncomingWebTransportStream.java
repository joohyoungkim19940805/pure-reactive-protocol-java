package com.byeolnaerim.prp.webtransport.kwik;

import java.io.InputStream;
import tech.kwik.core.QuicStream;

record KwikIncomingWebTransportStream(
    long sessionId,
    QuicStream quicStream,
    InputStream input,
    boolean bidirectional
) {}
