package com.lambdazen.bitsy;

import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.VertexBean;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

public class BitsyIoRegistryV3d0
    extends AbstractIoRegistry
{
  private static final BitsyIoRegistryV3d0 INSTANCE = new BitsyIoRegistryV3d0();

  private BitsyIoRegistryV3d0() {
    register(GryoIo.class, UUID.class, new UUIDGryoSerializer());
    register(GryoIo.class, VertexBean.class, new UUIDGryoSerializer());
    register(GryoIo.class, EdgeBean.class, new UUIDGryoSerializer());

    register(GraphSONIo.class, UUID.class, BitsyGraphSONModule.getInstance());
    register(GraphSONIo.class, VertexBean.class, BitsyGraphSONModule.getInstance());
    register(GraphSONIo.class, EdgeBean.class, BitsyGraphSONModule.getInstance());

    register(GraphBinaryIo.class, UUID.class, new UUIDGraphBinarySerializer());
    register(GraphBinaryIo.class, VertexBean.class, new UUIDGraphBinarySerializer());
    register(GraphBinaryIo.class, EdgeBean.class, new UUIDGraphBinarySerializer());
  }

  public static BitsyIoRegistryV3d0 instance() {
    return INSTANCE;
  }

  final static class UUIDGryoSerializer
      extends Serializer<UUID>
  {
    @Override
    public void write(final Kryo kryo, final Output output, final UUID uuid) {
      output.writeLong(uuid.getMostSignificantBits());
      output.writeLong(uuid.getLeastSignificantBits());
    }

    @Override
    public UUID read(final Kryo kryo, final Input input, final Class<UUID> aClass) {
      long msb = input.readLong();
      long lsb = input.readLong();
      return new UUID(msb, lsb);
    }
  }


}
