package com.lambdazen.bitsy.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdazen.bitsy.util.BufferPotential;

public class VEObsolescencePotential implements BufferPotential<IVeReorgJob> {
    private static final Logger log = LoggerFactory.getLogger(VEObsolescencePotential.class);
    
    double factor;
    long origLines;
    long addedLines;
    int minLinesPerReorg;
    
    public VEObsolescencePotential(int minLinesPerReorg, double factor, long origLines) {
        this.factor = factor;
        this.origLines = origLines;
        this.addedLines = 0;
        this.minLinesPerReorg = minLinesPerReorg;
    }
    
    @Override
    public boolean addWork(IVeReorgJob newWork) {
        if (newWork instanceof TxLog) {
            this.addedLines += ((TxLog)newWork).getReorgPotDiff();

            double factorTimesOrigLines = factor * origLines;

            log.debug("VE obsolescence potential: {}. Threshold is maximum of {} and {}", addedLines, factorTimesOrigLines, minLinesPerReorg);

            return (addedLines > factorTimesOrigLines) && (addedLines > minLinesPerReorg);
        } else {
            // Don't reorg
            return false;
        }
    }

    @Override
    public void reset() {
        addedLines = 0;
    }
    
    // This method is called by the flusher
    public void setOrigLines(int origLines) {
        this.origLines = origLines;
    }
    
    public double getFactor() {
        return factor;
    }

    public void setFactor(double factor) {
        this.factor = factor;
    }

    public int getMinLinesPerReorg() {
        return minLinesPerReorg;
    }

    public void setMinLinesPerReorg(int minLinesPerReorg) {
        this.minLinesPerReorg = minLinesPerReorg;
    }
}
