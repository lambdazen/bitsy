package com.lambdazen.bitsy.store;

import java.nio.file.Path;

import com.lambdazen.bitsy.BitsyException;

public class BackupJob extends JobWithCountDownLatch implements IVeReorgJob {
    private Path backupDir;
    private BitsyException bex;
    
    public BackupJob(Path backupDir) {
        this.backupDir = backupDir;
    }
    
    public Path getBackupDir() {
        return backupDir;
    }
    
    public BitsyException getException() {
        return bex;
    }

    public void setException(BitsyException bex) {
        this.bex = bex;
    }
}
