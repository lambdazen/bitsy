package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.CommittableFileLog;
import java.nio.ByteBuffer;

/** This class captures a transaction to be written to the TA/B transaction files */
public class TxUnit extends JobWithCountDownLatch {
    ByteBuffer vertices;
    ByteBuffer edges;
    ByteBuffer tx;
    BitsyException bex;

    public TxUnit(ByteBuffer vertices, ByteBuffer edges, ByteBuffer tx) {
        this.vertices = vertices;
        this.edges = edges;
        this.tx = tx;
    }

    public ByteBuffer getByteBufferForV() {
        vertices.position(0);

        return vertices;
    }

    public ByteBuffer getByteBufferForE() {
        edges.position(0);

        return edges;
    }

    public ByteBuffer getByteBufferForT() {
        tx.position(0);

        return tx;
    }

    public int writeToFile(CommittableFileLog cfl) {
        ByteBuffer vbb = getByteBufferForV();
        ByteBuffer ebb = getByteBufferForE();
        ByteBuffer tbb = getByteBufferForT();

        int size = vbb.remaining() + ebb.remaining() + tbb.remaining();

        cfl.append(vbb);
        cfl.append(ebb);
        cfl.append(tbb);

        return size;
    }

    public BitsyException getException() {
        return bex;
    }

    public void setException(BitsyException bex) {
        this.bex = bex;
    }
}
