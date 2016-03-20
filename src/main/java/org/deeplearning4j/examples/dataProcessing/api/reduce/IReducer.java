package org.deeplearning4j.examples.dataProcessing.api.reduce;

import org.canova.api.writable.Writable;
import org.deeplearning4j.examples.dataProcessing.api.schema.Schema;

import java.util.List;

/**A reducer aggregates or combines a set of examples into a single List<Writable>
 */
public interface IReducer {

    void setInputSchema(Schema schema);

    Schema transform(Schema schema);

    List<Writable> reduce(List<List<Writable>> examplesList);

}
