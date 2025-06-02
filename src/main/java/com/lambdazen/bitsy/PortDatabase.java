package com.lambdazen.bitsy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class ports the database files across major versions */
public class PortDatabase {
    public static final List<String> SUPPORTED_VERSIONS = Arrays.asList(new String[] {"1.0", "1.5"});

    private static final String[] FILE_NAMES =
            new String[] {"metaA.txt", "metaB.txt", "vA.txt", "vB.txt", "eA.txt", "eB.txt", "txA.txt", "txB.txt"};
    private static final Charset UTF8 = Charset.forName("UTF-8");

    String targetVersion;
    Path sourcePath;
    Path targetPath;
    String sourceVersion;
    String error = null;

    public PortDatabase(String[] args) throws IOException {
        if (args.length == 0) {
            setError("No arguments provided");
            return;
        } else if (args.length != 4) {
            setError("Expecting 4 arguments");
            return;
        }

        this.targetVersion = null;
        int i;
        for (i = 0; i < args.length - 1; i++) {
            if (args[i].equals("-toVersion")) {
                targetVersion = args[i + 1];
                break;
            }
        }

        String sourceDir = (i == 0) ? args[2] : args[0];
        String targetDir = (i == 2) ? args[1] : args[3];

        if (targetVersion == null) {
            setError("Could not find -toVersion flag followed by a version number");
            return;
        } else if (!SUPPORTED_VERSIONS.contains(targetVersion)) {
            setError("The version number " + targetVersion
                    + " provided in the -toVersion flag is not supported. You must provide one of the following: "
                    + SUPPORTED_VERSIONS);
            return;
        }

        this.sourcePath = Paths.get(sourceDir);
        this.targetPath = Paths.get(targetDir);

        if (!Files.isDirectory(sourcePath)) {
            setError("Source path " + sourceDir + " does not point to a directory");
            return;
        }

        if (!Files.isDirectory(targetPath)) {
            setError("Target path " + targetDir + " does not point to a directory");
            return;
        }

        this.sourceVersion = getVersion(sourcePath);
        if (sourceVersion == null) {
            return;
        }

        if (!SUPPORTED_VERSIONS.contains(sourceVersion)) {
            setError("The version number " + sourceVersion
                    + " found in the source database is not supported. You must provide a database created by one of these versions of Bitsy: "
                    + SUPPORTED_VERSIONS);
            return;
        }

        if (sourceVersion.equals(targetVersion)) {
            setError("The source and target version numbers are the same: Version " + sourceVersion);
            return;
        }

        System.out.println("Porting database in " + sourceDir + " from version " + sourceVersion + " to version "
                + targetVersion + " under " + targetDir);

        portDatabase();

        System.out.println("Success");
    }

    private void portDatabase() throws IOException {
        Converter converter;
        if (sourceVersion.equals("1.0") && targetVersion.equals("1.5")) {
            converter = new V10ToV15Coverter();
        } else if (sourceVersion.equals("1.5") && targetVersion.equals("1.0")) {
            converter = new V15ToV10Coverter();
        } else {
            setError("PortDatabase does not support porting from source version " + sourceVersion
                    + " to target version " + targetVersion);
            return;
        }

        for (String fileName : FILE_NAMES) {
            Path path = sourcePath.resolve(fileName);

            InputStream fis = null;
            BufferedReader br = null;
            OutputStream fos = null;

            try {
                fis = Files.newInputStream(path);
                fos = Files.newOutputStream(targetPath.resolve(fileName));
                br = new BufferedReader(new InputStreamReader(fis, UTF8));

                String line;
                int lineNo = 0;
                while ((line = br.readLine()) != null) {
                    String outLine = converter.convert(line, lineNo, fileName);
                    if (outLine != null) {
                        fos.write(outLine.getBytes(UTF8));
                        fos.write('\n');
                    }
                }
            } finally {
                if (br != null) {
                    br.close();
                }

                if (fis != null) {
                    fis.close();
                }

                if (fos != null) {
                    fos.close();
                }
            }
        }
    }

    private String getVersion(Path sourcePath) throws IOException {
        Path mA = sourcePath.resolve("metaA.txt");
        Path mB = sourcePath.resolve("metaB.txt");

        String version = "1.0";
        boolean missingFiles = true;
        if (Files.exists(mA)) {
            String versionA = getVersionFromPath(mA);
            if (versionA != null) {
                version = versionA;
            }
            missingFiles = false;
        }

        if (Files.exists(mB)) {
            String versionB = getVersionFromPath(mB);
            if (versionB != null) {
                version = versionB;
            }
            missingFiles = false;
        }

        if (missingFiles) {
            setError("Neither metaA.txt nor metaB.txt can be found in " + sourcePath);
            return null;
        } else {
            return version;
        }
    }

    public String getVersionFromPath(Path metaPath) throws IOException {
        String fileName = metaPath.toString();
        try (BufferedReader br = Files.newBufferedReader(metaPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNo = 0;
            while ((line = br.readLine()) != null) {
                lineNo++;

                int hashPos = line.lastIndexOf('#');
                if (hashPos < 0) {
                    throw new BitsyException(
                            BitsyErrorCodes.CHECKSUM_MISMATCH,
                            "Line " + lineNo + " in file " + fileName + " has no hash-code. Encountered " + line);
                } else {
                    String hashCode = line.substring(hashPos + 1);
                    String expHashCode = toHex(line.substring(0, hashPos + 1).hashCode());

                    if (!hashCode.endsWith(expHashCode)) {
                        throw new BitsyException(
                                BitsyErrorCodes.CHECKSUM_MISMATCH,
                                "Line " + lineNo + " in file " + fileName + " has the wrong hash-code " + hashCode
                                        + ". Expected " + expHashCode);
                    } else {
                        // All OK
                        char typeChar = line.charAt(0);
                        String version = line.substring(2, hashPos);

                        if (typeChar == 'M') {
                            return version;
                        }
                    }
                }
            }
        }

        return null;
    }

    private void setError(String error) {
        this.error = error;
    }

    private String getError() {
        return error;
    }

    private static void printUsage(String error) {
        if (error != null) {
            System.err.println("ERROR: " + error);
        }

        System.err.println(
                "Usage: java com.lambdazen.bitsy.PortDatabase -toVersion <target version number> <source directory> <target directory>");
    }

    public static void main(String[] args) {
        try {
            PortDatabase task = new PortDatabase(args);

            if (task.getError() != null) {
                printUsage(task.getError());
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Faster than Integer.toHexString()
    private static final char[] HEX_CHAR_ARR = "0123456789abcdef".toCharArray();

    private static String toHex(int input) {
        final char[] sb = new char[8];
        final int len = (sb.length - 1);
        for (int i = 0; i <= len; i++) { // MSB
            sb[i] = HEX_CHAR_ARR[((int) (input >>> ((len - i) << 2))) & 0xF];
        }
        return new String(sb);
    }

    public interface Converter {
        public String convert(String line, int lineNo, String fileName);
    }

    public class V10ToV15Coverter implements Converter {
        Pattern edgePat = Pattern.compile("^(E=\\{[^{}]*,\"p\":)\\[\"java.util.TreeMap\",(.*)\\]\\}#[0-9a-zA-Z]*$");

        @Override
        public String convert(String line, int lineNo, String fileName) {
            if (line.startsWith("H=") && fileName.startsWith("meta")) {
                String versionLine = "M=1.5#";
                return line + "\n" + versionLine + toHex(versionLine.hashCode());
            } else if (line.startsWith("E=")) {
                Matcher m = edgePat.matcher(line);
                if (!m.find()) {
                    return line;
                } else {
                    // Move from TreeMap to Map
                    line = m.group(1) + m.group(2) + "}#";

                    return line + toHex(line.hashCode());
                }
            } else {
                return line;
            }
        }
    }

    public class V15ToV10Coverter implements Converter {
        Pattern edgePat = Pattern.compile("^(E=\\{[^{}]*,\"p\":)(.*)\\}#[0-9a-zA-Z]*$");

        @Override
        public String convert(String line, int lineNo, String fileName) {
            if (line.startsWith("M=") && fileName.startsWith("meta")) {
                // Skip the version
                return null;
            } else if (line.startsWith("E=")) {
                Matcher m = edgePat.matcher(line);
                if (!m.find()) {
                    return line;
                } else {
                    // Move from TreeMap to Map
                    line = m.group(1) + "[\"java.util.TreeMap\"," + m.group(2) + "]}#";

                    return line + toHex(line.hashCode());
                }
            } else {
                return line;
            }
        }
    }
}
