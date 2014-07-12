/*
 * Copyright (C) 2013 Suresh Mahalingam.  All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package org.aredis.io;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * <p>
 * An OutputStream that compresses the data once the size crosses a certain threshold. The setCompressionEnabled can be used
 * to turn off compressions for sections that are not part of the data and should not be compressed. When compressionEnabled
 * is flipped from false to true a new compression session is started and the data size is counted from that point and when
 * it exceeds the threshold the stream is reset to that point, a GZIPOutputStream is started and the data in session is
 * written to the GZIPOutputStream. compressionEnabled is false for commands and keys. It is turned on by the DataHandler
 * if it supports compression.
 * </p>
 *
 * <p>
 * When gzip compression is on the stream does not store the original data. To avoid compressing data that is already
 * compressed by gzip or other means a compression ratio check is made after every 1024 bytes. If the compression ratio
 * is above the abandonCompressionRatio then the compression is abandoned. This is done by closing the GZIPOutputStream
 * resetting the stream back to the start of the compression session, writing back the original data by uncompressing
 * the data written and continuing with future writes without compression.
 * </p>
 * @author Suresh
 *
 */
public class CompressibleByteArrayOutputStream extends OutputStream {

    /**
     * Default Compression Threshold.
     */
    public static final int DEFAULT_COMPRESSION_THRESHOLD = 1024;

    private static final int GZIP_ABANDON_CHECK_BYTES = 1024;

    private static final int GZIP_ABANDON_CHECK_MAX_BYTES = 2048;

    private static class OpenPrintStream extends PrintStream {

        public OpenPrintStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close() {
            // Ignore
        }

    }

    private ReusableByteArrayOutputStream bop;

    private OutputStream gzipOp;

    private int compressionThreshold;

    private boolean compressionEnabled;

    private boolean abandonCompressionEnabled;

    private int abandonedGzipLen;

    private boolean compressionInfoCleared;

    private int countAtSessionStart;

    private int abandonRatioNr = 7;

    private int abandonRatioDr = 10;

    private int abandonCheckGzipLenLimit = Integer.MAX_VALUE / abandonRatioDr;

    private int abandonCheckLenLimit = Integer.MAX_VALUE / abandonRatioNr;

    private int bytesWrittenSinceSessionStart;

    private int bytesWrittenAtLastAbandonCheck;

    private Map<String, PrintStream> charSetPrintStreamMap = new HashMap<String, PrintStream>();

    /**
     * Creates a new CompressibleByteArrayOutputStream.
     * @param pcompressionThreshold Compression Threshold in bytes
     */
    public CompressibleByteArrayOutputStream(int pcompressionThreshold) {
        bop = new ReusableByteArrayOutputStream(pcompressionThreshold < 32 ? 32 : pcompressionThreshold > DEFAULT_COMPRESSION_THRESHOLD ? DEFAULT_COMPRESSION_THRESHOLD : pcompressionThreshold);
        compressionThreshold = pcompressionThreshold;
        compressionEnabled = true;
        abandonCompressionEnabled = true;
    }

    /**
     * Creates a new CompressibleByteArrayOutputStream.
     * @param size Initial buffer capacity
     * @param pcompressionThreshold Compression Threshold in bytes
     */
    public CompressibleByteArrayOutputStream(int size, int pcompressionThreshold) {
        bop = new ReusableByteArrayOutputStream(size);
        compressionThreshold = pcompressionThreshold;
        compressionEnabled = true;
        abandonCompressionEnabled = true;
    }

    /**
     * Creates a new CompressibleByteArrayOutputStream with the default compression threshold of 1024 bytes.
     */
    public CompressibleByteArrayOutputStream() {
        this(DEFAULT_COMPRESSION_THRESHOLD);
    }

    /**
     * Resets the stream by clearing any existing data. The compressionEnabled flag is unchanged.
     */
    public void reset() {
        bop.reset();
        gzipOp = null;
    }

    private void resetPrintStream(PrintStream ps) {
        if(ps == null) {
            throw new NullPointerException("PrintStream is null");
        }
        boolean found = false;
        for(PrintStream testPs : charSetPrintStreamMap.values()) {
            if(ps == testPs) {
                found = true;
                break;
            }
        }
        if(!found) {
            throw new RuntimeException("PrintStream to reset was not created by this CompressibleByteArrayInputStream");
        }
        int savedCount = bop.getCount();
        ps.println("C");
        ps.flush();
        bop.setCount(savedCount);
    }

    /**
     * Gets a printStream that writes to this Stream corresponding to a given Charset. The returned PrintStream
     * is a sub-class of PrintStream that ignores closing of the stream. The same PrintStream is returned for the
     * Same charSet for calls to getPrintStream on a given CompressionbleByteArrayOutputStream.
     * @param charSet CharSet to use
     * @return PrintStream that writes to this stream
     */
    public PrintStream getPrintStream(String charSet) {
        if(charSet == null) {
            throw new NullPointerException("CharSet is null");
        }
        PrintStream ps = charSetPrintStreamMap.get(charSet);
        if(ps == null) {
            ps = new OpenPrintStream(this);
            charSetPrintStreamMap.put(charSet, ps);
        }
        else {
            resetPrintStream(ps);
        }

        return ps;
    }

    private void setCompressionIfRequired(int len) throws IOException {
        int count = bop.getCount();
        int bytesInSession = count - countAtSessionStart;
        if(compressionEnabled && abandonedGzipLen == 0 && compressionThreshold >= 0 && gzipOp == null && bytesInSession + len >= compressionThreshold) {
            // Turn on compression. Write bytes in session to end of bop and
            // then copy it to session start
            byte [] buf = bop.getBuf();
            OutputStream gzipOutput = new BufferedOutputStream(new GZIPOutputStream(bop), 2048);
            gzipOutput.write(buf, countAtSessionStart, bytesInSession);
            // Reassign buf in case bop has expanded
            buf = bop.getBuf();
            int gzipLen = bop.getCount() - count;
            System.arraycopy(buf, count, buf, countAtSessionStart, gzipLen);
            bop.setCount(countAtSessionStart + gzipLen);
            gzipOp = gzipOutput;
            if(bytesInSession >= GZIP_ABANDON_CHECK_BYTES) {
                abandonCompressionIfRequired();
            }
        }
    }

    private void abandonCompressionIfRequired() throws IOException {
        int bytesRead, count = bop.getCount();
        int gzipLen = count - countAtSessionStart;
        int len = bytesWrittenSinceSessionStart;
        bytesWrittenAtLastAbandonCheck = bytesWrittenSinceSessionStart;
        if(abandonCompressionEnabled && gzipLen * abandonRatioDr >= len * abandonRatioNr && gzipLen <= abandonCheckGzipLenLimit && len <= abandonCheckLenLimit) {
            if(gzipOp != null) {
                gzipOp.close();
                gzipOp = null;
                count = bop.getCount();
                gzipLen = count - countAtSessionStart;
            }
            abandonedGzipLen = gzipLen;
            byte compressedData[] = new byte[gzipLen];
            byte[] buf = bop.getBuf();
            System.arraycopy(buf, countAtSessionStart, compressedData, 0, gzipLen);
            GZIPInputStream gzipIp = new GZIPInputStream(new ByteArrayInputStream(compressedData));
            int finalCount = countAtSessionStart + len;
            while(buf.length < finalCount) {
                bop.setCount(buf.length);
                bop.write(0); // Expand Buffer
                buf = bop.getBuf();
            }
            int ofs = countAtSessionStart;
            while((bytesRead = gzipIp.read(buf, ofs, finalCount - ofs)) > 0) {
                ofs += bytesRead;
                if(ofs >= finalCount) {
                    break;
                }
            }
            if(ofs != finalCount) {
                throw new IOException("Internal count mismatch ofs = " + ofs + " finalCount = " + finalCount);
            }
            bop.setCount(finalCount);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        try {
            setCompressionIfRequired(len);
            do {
                if(gzipOp != null) {
                    int bytesWritten = len;
                    boolean doAbandonCheck = false;
                    int bytesSinceAbandonCheck;
                    if((bytesSinceAbandonCheck = bytesWrittenSinceSessionStart - bytesWrittenAtLastAbandonCheck + len) >= GZIP_ABANDON_CHECK_BYTES) {
                        doAbandonCheck = true;
                        if(bytesSinceAbandonCheck >= GZIP_ABANDON_CHECK_MAX_BYTES) {
                            bytesWritten = GZIP_ABANDON_CHECK_MAX_BYTES + bytesWrittenAtLastAbandonCheck - bytesWrittenSinceSessionStart;
                        }
                    }
                    gzipOp.write(b, off, bytesWritten);
                    bytesWrittenSinceSessionStart += bytesWritten;
                    off += bytesWritten;
                    len -= bytesWritten;
                    if(doAbandonCheck) {
                        abandonCompressionIfRequired();
                    }
                }
                else {
                    bop.write(b, off, len);
                    bytesWrittenSinceSessionStart += len;
                    len = 0;
                }
            }
            while(len > 0);
        }
        catch(IOException e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    @Override
    public void write(int b) {
        try {
            bytesWrittenSinceSessionStart++;
            setCompressionIfRequired(1);
            if(gzipOp != null) {
                gzipOp.write(b);
                if(bytesWrittenSinceSessionStart - bytesWrittenAtLastAbandonCheck >= GZIP_ABANDON_CHECK_BYTES) {
                    abandonCompressionIfRequired();
                }
            }
            else {
                bop.write(b);
            }
        } catch (IOException e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    @Override
    public void close() throws IOException {
        if(gzipOp != null) {
            gzipOp.close();
            gzipOp = null;
            abandonCompressionIfRequired();
        }
        setCompressionEnabled(false);
    }

    /**
     * Writes the data in the Stream to an OutputStream
     * @param op OutputStream to write the data to
     * @throws IOException In case of an error during write
     * @throws IllegalStateException if compression is currently in effect on the stream
     */
    public void writeTo(OutputStream op) throws IOException {
        if(gzipOp != null) {
            throw new IllegalStateException("Cannot writeTo with compression on");
        }
        bop.writeTo(op);
    }

    /**
     * Checks if compression is currently in effect. It might return false even if compressionEnabled is set but
     * the data size is less than the compressionThreshold. It will also return false if the compression has been
     * abandoned.
     * @return true if compression is happening on the stream, false otherwise
     */
    public boolean isCompressed() {
        return gzipOp != null;
    }

    /**
     * Low level method to get the count of the data in the buffer.
     * @return Count
     */
    public int getCount() {
        return bop.getCount();
    }

    /**
     * Low level method to set the count in the buffer.
     * @param pcount New count
     * @throws IllegalStateException if compression is currently in effect on the stream
     */
    public void setCount(int pcount) {
        if(gzipOp != null) {
            throw new IllegalStateException("Cannot set count with compression on");
        }
        int diff = pcount - bop.getCount();
        bop.setCount(pcount);
        bytesWrittenSinceSessionStart += diff;
        bytesWrittenAtLastAbandonCheck += diff;
    }

    /**
     * Gets the underlying buffer.
     * @return buffer used to store the data
     */
    public byte [] getBuf() {
        return bop.getBuf();
    }

    /**
     * Gets the compression threshold.
     * @return CompressionThreshold
     */
    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    /**
     * Sets the compression threshold.
     * @param pcompressionThreshold New Compression Threshold
     */
    public void setCompressionThreshold(int pcompressionThreshold) {
        compressionThreshold = pcompressionThreshold;
        try {
            setCompressionIfRequired(0);
        } catch (IOException e) {
            throw new RuntimeException("Internal error", e);
        }
    }

    /**
     * Checks if compression is enabled.
     * @return true if compression is enabled
     */
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Returns the current compression info as a String. Returns info on the data sizes before and after the compression.
     * Also returns info regarding abandoning compression. Returns null if no compression was done.
     * This should be called after the current compression session has been ended by calling setCompressionEnabled(false).
     * @return  Compression Info
     */
    public String getCompressionInfo() {
        String msg = null;
        if (!compressionInfoCleared) {
            int bytesInSession = bop.getCount() - countAtSessionStart;
            if(bytesInSession != bytesWrittenSinceSessionStart) {
                msg = "Compressed data from " + bytesWrittenSinceSessionStart + " to " + bytesInSession + " bytes";
            }
            else if(abandonedGzipLen > 0) {
                msg = "Abandoned GZIP because " + bytesWrittenAtLastAbandonCheck + " bytes compressed only to " + abandonedGzipLen + " (" + ((int) Math.round(100 * (double) abandonedGzipLen / bytesWrittenAtLastAbandonCheck)) + "%). Final Data Length = " + bytesWrittenSinceSessionStart + "(=" + bytesInSession + ')';
            }
        }
        return msg;
    }

    public boolean clearCompressionInfo() {
        boolean prevValue = compressionInfoCleared;
        compressionInfoCleared = true;
        return prevValue;
    }

    /**
     * Sets if compression is enabled. If compression was previously disabled then a new compression session is started.
     * @param pcompressionEnabled whether compression should be enabled
     */
    public void setCompressionEnabled(boolean pcompressionEnabled) {
        if(!compressionEnabled && pcompressionEnabled) {
            // Re-enable abandonCompression if it has been disabled explicitly via setAbandonCompressionEnabled
            abandonCompressionEnabled = true;
            countAtSessionStart = bop.getCount();
            abandonedGzipLen = 0;
            bytesWrittenSinceSessionStart = 0;
            bytesWrittenAtLastAbandonCheck = 0;
            compressionInfoCleared = false;
        }
        compressionEnabled = pcompressionEnabled;
    }

    /**
     * Checks if abandon compression is enabled.
     * @return true if abandon compression is enabled. False otherwise
     */
    public boolean isAbandonCompressionEnabled() {
        return abandonCompressionEnabled;
    }

    /**
     * Sets if abandon compression should be enabled. This flag is automatically set if compression is
     * re-enabled by calling setCompressionEnabled(true).
     * @param pabandonCompressionEnabled true if abandon compression should be enabled, false otherwise.
     */
    public void setAbandonCompressionEnabled(boolean pabandonCompressionEnabled) {
        abandonCompressionEnabled = pabandonCompressionEnabled;
    }

    /**
     * Low level method to get the count at the start of the current compression session.
     * @return count value when compression was last enabled on this stream
     */
    public int getCountAtSessionStart() {
        return countAtSessionStart;
    }

    /**
     * Gets the abandon compression ratio. If the compression ratio goes above this the compression is abandoned. The default is 0.7.
     * @return Abandon Compression Ratio
     */
    public double getAbandonCompressionRatio() {
        return abandonRatioNr / (double) abandonRatioDr;
    }

    /**
     * Sets the abandon compression ratio. Only 2 decimal places are considered.
     * @param ratio New Abandon Compression ratio
     */
    // Optimizing on double computation by taking 2 decimals and converting to Numerator and Denominator
    public void setAbandonCompressionRatio(double ratio) {
        if(ratio < 0.2 || ratio > 2) {
            throw new IllegalArgumentException("abandon compression ratio should be between 0.3 and 2");
        }
        abandonRatioNr = (int) (ratio * 100 + 0.5f);
        abandonRatioDr = 100;
        // Take out common factors in Numerator and denominaotor by dividing by 2 and 5
        while((abandonRatioNr & 1) == 0 && (abandonRatioDr & 1) == 0) {
            abandonRatioNr >>= 1;
            abandonRatioDr >>= 1;
        }
        while(abandonRatioNr % 5 == 0 && abandonRatioDr % 5 == 0) {
            abandonRatioNr /= 5;
            abandonRatioDr /= 5;
        }
        // Compute Integer overflow limit
        abandonCheckGzipLenLimit = Integer.MAX_VALUE / abandonRatioDr;

        abandonCheckLenLimit = Integer.MAX_VALUE / abandonRatioNr;
    }
}
