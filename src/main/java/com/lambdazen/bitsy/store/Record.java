package com.lambdazen.bitsy.store;

import java.io.IOException;
import java.io.StringWriter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.IGraphStore;
import com.lambdazen.bitsy.UUID;

/** This class represents a line in a text file captured in the DB */
public class Record {
    private static final char[] HEX_CHAR_ARR = "0123456789abcdef".toCharArray();
    public static final String newLine = "\n";
    //private static final ObjectMapper mapper = new ObjectMapper();
    private static final JsonFactory factory = new JsonFactory();

    public static enum RecordType {H, // Header
        L, // Log marker
        V, // Vertex
        E, // Edges
        T, // Transaction
        I, // Index -- stored in meta?.txt files
        M}; // Major version -- stored in meta?.txt files

    private static final char[] recordChars = new char[] {'H', 'L', 'V', 'E', 'T', 'I', 'M'};
    private static final RecordType[] recordTypes = new RecordType[] {RecordType.H, RecordType.L, RecordType.V, RecordType.E, RecordType.T, RecordType.I, RecordType.M};
    private static final int numRecChars = recordChars.length;

    RecordType type;
    String json;
    BitsyEdge edge;
    BitsyVertex vertex;

    public static boolean IS_ANDROID = "The Android Project".equals(System.getProperty("java.specification.vendor"));
    public static int ANDROID_EOR = 1234567890;

    public static int hashCode(String str) {
        if (!IS_ANDROID) {
                // Backward compatible for non-Android systems
                return str.hashCode();
        } else {
            return ANDROID_EOR;
        }
    }

    public Record(RecordType type, String json) {
        this.type = type;
        this.json = json;
    }

    public void deserialize(ObjectReader vReader, ObjectReader eReader) throws JsonProcessingException, IOException {
        if ((type == RecordType.V) && (vReader != null)) {
            VertexBeanJson vBean = vReader.readValue(json);
            this.vertex = new BitsyVertex(vBean, null, vBean.getState());
        }

        if ((type == RecordType.E) && (eReader != null)) {
            EdgeBeanJson eBean = eReader.readValue(json);
            this.edge = new BitsyEdge(eBean, null, eBean.getState());
        }
    }

    public RecordType getType() {
        return type;
    }

    public String getJson() {
        return json;
    }

    // Efficient method to write a vertex -- avoids writeValueAsString
    public static void generateVertexLine(StringWriter sw, ObjectMapper mapper, VertexBean vBean) throws JsonGenerationException, JsonMappingException, IOException {
        sw.getBuffer().setLength(0);

        sw.append('V'); // Record type
        sw.append('=');

        mapper.writeValue(sw, vBean);

        sw.append('#');

        int hashCode = hashCode(sw.toString());
        sw.append(toHex(hashCode));
        sw.append('\n');
    }

    // Efficient method to write an edge -- avoids writeValueAsString
    public static void generateEdgeLine(StringWriter sw, ObjectMapper mapper, EdgeBean eBean) throws JsonGenerationException, JsonMappingException, IOException {
        sw.getBuffer().setLength(0);

        sw.append('E'); // Record type
        sw.append('=');

        mapper.writeValue(sw, eBean);

        sw.append('#');

        int hashCode = hashCode(sw.toString());
        sw.append(toHex(hashCode));
        sw.append('\n');
    }

    public static String generateDBLine(RecordType type, String line) {
        String dbLine = type + "=" + line + "#";
        int hashCode = hashCode(dbLine);

        return dbLine + toHex(hashCode) + newLine;
    }

    public static Record parseRecord(String dbLine, int lineNo, String fileName) {
        int hashPos = dbLine.lastIndexOf('#');
        if (hashPos < 0) {
            throw new BitsyException(BitsyErrorCodes.CHECKSUM_MISMATCH, "Line " + lineNo + " in file " + fileName + " has no hash-code. Encountered " + dbLine);
        } else {
            String hashCode = dbLine.substring(hashPos + 1);
            String expHashCode = toHex(hashCode(dbLine.substring(0, hashPos + 1)));

            if ((hashCode == null) || !hashCode.trim().equals(expHashCode)) { // TODO: Currently DB is not portable between Android and Java
                throw new BitsyException(BitsyErrorCodes.CHECKSUM_MISMATCH, "Line " + lineNo + " in file " + fileName + " has the wrong hash-code " + hashCode + ". Expected " + expHashCode);
            } else {
                // All OK
                RecordType type = typeFromChar(dbLine.charAt(0));
                String json = dbLine.substring(2, hashPos);
                return new Record(type, json);
            }
        }
    }

    // Faster than RecordType.valueof()
    private static RecordType typeFromChar(char recChar) {
        for (int i=0; i < numRecChars; i++) {
            if (recordChars[i] == recChar) {
                return recordTypes[i];
            }
        }

        throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unrecognized record type " + recChar);
    }

    // Faster than Integer.toHexString()
    private static String toHex(int input) {
        final char[] sb = new char[8];
        final int len = (sb.length-1);
        for (int i = 0; i <= len; i++) { // MSB
            sb[i] = HEX_CHAR_ARR[((int)(input >>> ((len - i)<<2))) & 0xF];
        }
        return new String(sb);
    }

    // This method checks to see if a record is obsolete, i.e., its version is not present in the store
    public boolean checkObsolete(IGraphStore store, boolean isReorg, int lineNo, String fileName) {
        if (type == RecordType.T) {
            // Transaction boundaries are obsolete during reorg
            return isReorg;
        } else if (type == RecordType.L) {
            // A log is always obsolete
            return false;
        } else if ((type != RecordType.V) && (type != RecordType.E)) {
            throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unhanded record type: " + type);
        }

        // A V or E record
        UUID id = null;
        int version = -1;
        String state = null;
        JsonToken token;

        try {
            JsonParser parser = factory.createJsonParser(json);

            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                // Find the version
                if (token == JsonToken.FIELD_NAME) {
                    if (parser.getCurrentName().equals("id")) {
                        parser.nextToken();
                        id = UUID.fromString(parser.getText());
                        continue;
                    }

                    if (parser.getCurrentName().equals("v")) {
                        parser.nextToken();
                        version = parser.getIntValue();
                        continue;
                    }

                    if (parser.getCurrentName().equals("s")) {
                        parser.nextToken();
                        state = parser.getText();

                        // No need to proceed further
                        break;
                    }
                }
            }

            if ((id == null) || (version == -1) || (state == null)) {
                throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unable to parse record '" + json + "' in file " + fileName + " at line " + lineNo);
            }

            if (state.equals("D")) {
                // Deleted -- can be ignored only on re-orgs
                return isReorg;
            } else {
                if (type == RecordType.V) {
                    VertexBean curV = store.getVertex(id);
                    if (curV == null) {
                        // Doesn't exist anymore, probably deleted later
                        return true;
                    } else if (curV.getVersion() != version) {
                        // Obsolete
                        return true;
                    } else {
                        // Good to go
                        return false;
                    }
                } else {
                    assert (type == RecordType.E);

                    EdgeBean curE = store.getEdge(id);
                    if (curE == null) {
                        // Doesn't exist anymore, probably deleted later
                        return true;
                    } else if (curE.getVersion() != version) {
                        // Obsolete
                        return true;
                    } else {
                        // Good to go
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Possible bug in code. Error serializing line '" + json + "' in file " + fileName + " at line " + lineNo, e);
        }
    }

    public BitsyVertex getVertex() {
        return vertex;
    }

    public BitsyEdge getEdge() {
        return edge;
    }
}
