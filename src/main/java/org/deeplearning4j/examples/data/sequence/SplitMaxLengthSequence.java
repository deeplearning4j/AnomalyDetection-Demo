package org.deeplearning4j.examples.data.sequence;

import org.canova.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**Split a sequence into a number of smaller sequences of length 'maxSequenceLength'.
 * If the sequence length is smaller than maxSequenceLength, the sequence is unchanged
 * Created by Alex on 16/03/2016.
 */
public class SplitMaxLengthSequence implements SequenceSplit {

    private final int maxSequenceLength;
    private final boolean equalSplits;

    /**
     * @param maxSequenceLength    max length of sequences
     * @param equalSplits   if true: split larger sequences inte equal sized subsequences. If false: split into
     */
    public SplitMaxLengthSequence(int maxSequenceLength, boolean equalSplits){
        this.maxSequenceLength = maxSequenceLength;
        this.equalSplits = equalSplits;
    }

    public List<Collection<Collection<Writable>>> split(Collection<Collection<Writable>> sequence) {
        int n = sequence.size();
        if(n <= maxSequenceLength) return Collections.singletonList(sequence);
        int splitSize;
        if(equalSplits){
            if(n % maxSequenceLength == 0){
                splitSize = n / maxSequenceLength;
            } else {
                splitSize = n / maxSequenceLength + 1;
            }
        } else {
            splitSize = maxSequenceLength;
        }

        List<Collection<Collection<Writable>>> out = new ArrayList<>();
        List<Collection<Writable>> current = new ArrayList<>(splitSize);
        for(Collection<Writable> step : sequence ){
            if(current.size() >= splitSize ){
                out.add(current);
                current = new ArrayList<>(splitSize);
            }
            current.add(step);
        }
        out.add(current);

        return out;
    }
}
