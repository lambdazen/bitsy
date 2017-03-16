package com.lambdazen.bitsy.store;

import com.fasterxml.jackson.databind.ObjectReader;
import com.lambdazen.bitsy.util.CommittableFileLog;

public class RecordReader {
    CommittableFileLog cfl;
    String fileName;
    int lineNo = 1;

    ObjectReader vReader;
    ObjectReader eReader;
    
    public RecordReader(CommittableFileLog cfl, ObjectReader vReader, ObjectReader eReader) {
        this.cfl = cfl;
        this.fileName = cfl.getPath().toString();
        this.vReader = vReader;
        this.eReader = eReader;
    }
    
    public Record next() throws Exception {
        String line = cfl.readLine();
        
        if (line == null) {
            return null;
        } else {
            lineNo++;
            
            Record ans = Record.parseRecord(line, lineNo, fileName);
            ans.deserialize(vReader, eReader);
            return ans;
        }
    }
}
