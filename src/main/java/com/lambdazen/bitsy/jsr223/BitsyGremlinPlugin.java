package com.lambdazen.bitsy.jsr223;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyElement;
import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyProperty;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.BitsyVertexProperty;
import com.lambdazen.bitsy.ThreadedBitsyGraph;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public class BitsyGremlinPlugin
    extends AbstractGremlinPlugin
{
  private static final String NAME = "lambdazen.bitsy";

  private static final ImportCustomizer imports() {
    return DefaultImportCustomizer.build()
        .addClassImports(BitsyEdge.class,
            BitsyElement.class,
            BitsyGraph.class,
            ThreadedBitsyGraph.class,
            BitsyProperty.class,
            BitsyVertex.class,
            BitsyVertexProperty.class,
            BitsyTransaction.class).create();
  }

  private static final BitsyGremlinPlugin INSTANCE = new BitsyGremlinPlugin();

  public BitsyGremlinPlugin() {
    super(NAME, imports());
  }

  public static BitsyGremlinPlugin instance() {
    return INSTANCE;
  }
}
