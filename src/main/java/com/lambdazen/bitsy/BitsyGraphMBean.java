package com.lambdazen.bitsy;

public interface BitsyGraphMBean {
    /**
     * Returns the reorgFactor which typically determines when the V?.txt and
     * E?.txt file will be reorganized. Reorganization is triggered only when the
     * total number of new vertices and edges added is more than the factor
     * multiplied by the original number of vertices and edges. Default value is 1.
     */
    public double getReorgFactor();

    /**
     * Set the reorgFactor. A higher number indicates fewer file operations, but
     * more disk space and startup time in the worst case. Default value is 1.
     */
    public void setReorgFactor(double factor);
    
    /**
     * Returns the minimum number of vertices and edges that must be added
     * before a reorganization is considered. This rule is used in combination
     * with the reorgFactor. Default value is 1000.
     */
    public int getMinLinesPerReorg();
    
    /**
     * Modify the minimum lines to be added before a reorganization is
     * considered. Default value is 1000. 
     */
    public void setMinLinesPerReorg(int minLinesPerReorg);
    
    /**
     * Returns the transaction log threshold which is the minimum size of the
     * transaction log (T?.txt) in bytes, before which the contents of the log
     * are copied to V?.txt and E?.txt. Default value is 4MB.
     */
    public long getTxLogThreshold();
    
    /**
     * Modify the transaction log threshold. A higher number indicates fewer
     * file operations, but more disk space and startup time in the worst case.
     * Default value is 4MB.
     */
    public void setTxLogThreshold(long txLogThreshold);
    
    /** This method flushes the transaction log to the V/E text files */
    public void flushTxLog();
    
    /** This method backs up the database while it is still operational. Only one backup can be in progress at a time. 
     * 
     * @param pathToDir directory to which the database must be backed up.  
     */
    public void backup(String pathToDir);
}
