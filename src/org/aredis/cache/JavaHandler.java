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

package org.aredis.cache;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import org.aredis.io.ClassDescriptorStorage;
import org.aredis.io.ClassDescriptors;
import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.io.OptiObjectInputStream;
import org.aredis.io.OptiObjectOutputStream;
import org.aredis.io.RedisConstants;
import org.aredis.io.ReusableByteArrayInputStream;
import org.aredis.net.ServerInfo;

/**
 * The default Data Handler. Strings are encoded as it is except if the encoded String begins with one of the special
 * headers ESCAPE_MARKER, GZIP_HEADER or OBJECT_STREAM_HEADER. The encoded String is gzipped if the size exceeds the
 * compression threshold.
 * If serializeNumbers property is set to false which is the default, Numbers like Integer and Double are also stored
 * as Strings instead of serializing them.
 * Other Objects are serialized using Java Serialization and gzipped if the size crosses the compression threshold.
 * When using java Serialization since we start with a new ObjectOutputStream every time the Class Descriptors used
 * to describe the classes of the Objects being serialized is written for every serialization.
 * If optimizeObjectStorage is set to true the Class Descriptors are maintained as an array in a common storage and
 * only the indexes into that array are written.
 * The storage is updated as and when new Class Descriptors are encountered. The default storage is the same redis
 * server under a key called JAVA_CL_DESCRIPTORS. This can be changed to use a common redis server by using
 * {@link RedisClassDescriptorStorageFactory} instead of the default {@link PerConnectionRedisClassDescriptorStorageFactory}.
 * Normal and optimized serialization are prefixed by different headers
 * so that they can be Serialized by a JavaHandler with or without the optimzeObjectStorage set.
 * @author Suresh
 *
 */
public class JavaHandler implements DataHandler {

    private static enum StreamType {STRING, GZIP, OBJECT, OPTI_OBJECT};

    public static final int ESCAPE_MARKER = 0x8080;

    public static final int GZIP_HEADER = 0x1f8b;

    public static final int OBJECT_STREAM_HEADER = 0xaced;

    private static final int MAX_TRIES = 300;

    /**
     * Factory to get an appropriate {@link ClassDescriptorStorage} when optimizeObjectStorage is enabled.
     */
    protected ClassDescriptorStorageFactory classDescriptorStorageFactory;

    /**
     * Indicates if Object Serialization is optimized by storing the Class Descriptors in a common location.
     */
    protected boolean optimizeObjectStorage;

    /**
     * Indicates whether to serialize numbers.
     */
    protected boolean serializeNumbers;

    private int stringCompressionThreshold;

    private int objectCompressionThreshold;

    private Charset charEncoding;

    /**
     * Creates a JavaHandler without optimeObjectStorage set to false.
     */
    public JavaHandler() {
        charEncoding = RedisConstants.UTF_8_CHARSET;
        stringCompressionThreshold = -2;
        objectCompressionThreshold = -2;
    }

    /**
     * Creates a JavaHandler.
     * @param pclassDescriptorStorageFactory Factory to Store ClassDescriptors. This must be non null if optimizeObjectStorage parameter is passed as true.
     * @param poptimizeObjectStorage true for Optimizing Object Storage (OPTI_JAVA_HANDLER), false for regular java serialization
     */
    public JavaHandler(ClassDescriptorStorageFactory pclassDescriptorStorageFactory, boolean poptimizeObjectStorage) {
        this();
        classDescriptorStorageFactory = pclassDescriptorStorageFactory;
        optimizeObjectStorage = poptimizeObjectStorage;
        if(pclassDescriptorStorageFactory == null && poptimizeObjectStorage) {
            throw new NullPointerException("ClassDescriptorStorage Factory cannot be null when optimizeObjectStorage is true");
        }
    }

    private void writeObject(CompressibleByteArrayOutputStream op, Object o) throws IOException {
        ObjectOutputStream objOp = new ObjectOutputStream(op);
        objOp.writeObject(o);
        objOp.close();
    }

    private Object readObject(InputStream ip, ServerInfo serverInfo) throws IOException, ClassNotFoundException {
        ObjectInputStream oip = new ObjectInputStream(ip);
        Object o = oip.readObject();
        return o;
    }

    private Object optiReadObject(InputStream ip, ServerInfo serverInfo)
            throws IOException, ClassNotFoundException {
        if(classDescriptorStorageFactory == null) {
            throw new IOException("Cannot deserialize OptiJavaHandler saved Objects without classDescriptorStorageFactory");
        }
        ClassDescriptorStorage descriptorsStorage = classDescriptorStorageFactory.getStorage(serverInfo);
        OptiObjectInputStream oip = new OptiObjectInputStream(ip, descriptorsStorage);
        Object o = oip.readObject();
        oip.close();
        return o;
    }

    private void optiWriteObject(CompressibleByteArrayOutputStream op, Object o, ServerInfo serverInfo)
            throws IOException {
        ClassDescriptorStorage descriptorsStorage = classDescriptorStorageFactory.getStorage(serverInfo);
        int tryCount, count = op.getCount();
        boolean tryAgain;
        tryCount = 0;
        do {
            OptiObjectOutputStream objOp = new OptiObjectOutputStream(op, descriptorsStorage);
            objOp.writeObject(o);
            objOp.close();
            ClassDescriptors updatedDescriptors = objOp.getUpdatedDescriptors();
            tryAgain = false;
            if(updatedDescriptors != null) {
                if(!descriptorsStorage.updateClassDescriptors(updatedDescriptors)) {
                    tryAgain = true;
                    tryCount++;
                    op.setCount(count);
                }
            }
        } while(tryAgain && tryCount < MAX_TRIES);
        if(tryAgain) {
            throw new IOException("Failed to update descriptors");
        }
    }

    @Override
    public void serialize(Object data, Object metaData, CompressibleByteArrayOutputStream op, ServerInfo serverInfo) throws IOException {
        op.setCompressionEnabled(true);
        String s = null;
        boolean checkStringStartingBytes = true;
        int compressionThreshold = objectCompressionThreshold;
        if(data instanceof String) {
            s = (String) data;
            compressionThreshold = stringCompressionThreshold;
        }
        else if(!serializeNumbers && data instanceof Number) {
            s = data.toString();
            op.setCompressionEnabled(false);
            checkStringStartingBytes = false;
        }
        if(compressionThreshold >= -1) {
            op.setCompressionThreshold(compressionThreshold);
        }
        else {
            compressionThreshold = op.getCompressionThreshold();
        }
        if(s != null) {
            if(checkStringStartingBytes) {
                int len, firstTwoBytes;
                boolean writeEscapeMarker = false;
                String subStr = s;
                len = s.length();
                if(len > 4) {
                    subStr = s.substring(0, 4);
                }
                if(len > 1) {
                    byte[] buf = subStr.getBytes(charEncoding);
                    if(buf.length >= 2) {
                        firstTwoBytes = ((((int) buf[0]) << 8) | (buf[1] & 0xFF)) & 0xffff;
                        if(firstTwoBytes == ESCAPE_MARKER) {
                            writeEscapeMarker = true;
                        }
                        else if(firstTwoBytes == GZIP_HEADER && compressionThreshold >= 0 && len < compressionThreshold) {
                            writeEscapeMarker = true;
                        }
                    }
                }
                if(writeEscapeMarker) {
                    op.write(ESCAPE_MARKER >> 8);
                    op.write(ESCAPE_MARKER & 0xFF);
                }
            }
            op.write(s.getBytes(charEncoding));
        }
        else {
            // Write ESCAPE_MARKER, 00/01 to indicate Regular/Optimized Stream differentiate when deserializing
            op.write(ESCAPE_MARKER >> 8);
            op.write(ESCAPE_MARKER & 0xFF);
            op.write(0);
            // Write Object
            if(optimizeObjectStorage) {
                op.write(1);
                optiWriteObject(op, data, serverInfo);
            }
            else {
                op.write(0);
                writeObject(op, data);
            }
        }
    }

    // Positions stream at a point ready to read Object or String after discarding ESCAPE_MARKER/Optimize Marker
    private StreamType checkObjectStream(InputStream ip) throws IOException {
        ip.mark(2);
        int firstTwoBytes = ip.read();
        firstTwoBytes = (firstTwoBytes << 8) | ip.read();
        StreamType status = StreamType.STRING;
        switch(firstTwoBytes) {
          case GZIP_HEADER:
            status = StreamType.GZIP;
            ip.reset();
            break;
          case ESCAPE_MARKER:
            ip.mark(2);
            int marker = ip.read();
            marker = (marker << 8) | ip.read();
            if(marker == 0) {
                status = StreamType.OBJECT;
            }
            else if(marker == 1) {
                status = StreamType.OPTI_OBJECT;
            }
            else {
                ip.reset();
            }
            break;
          default:
            ip.reset();
            break;
        }

        return status;
    }

    @Override
    public Object deserialize(Object metaData, byte [] b, int offset, int len, ServerInfo serverInfo) throws IOException {
        if(offset < 0 || len <= 0 || offset + len > b.length) {
            throw new IndexOutOfBoundsException("Invalid values, ofs: " + offset + ", len: " + len + ", b.length: " + b.length);
        }
        ReusableByteArrayInputStream rip = new ReusableByteArrayInputStream(b, offset, len);
        InputStream ip = rip;
        Object o = null;
        StreamType streamType = checkObjectStream(ip);
        if(streamType == StreamType.STRING) {
            int strStart = rip.getPos();
            int strLen = rip.getCount() - strStart;
            o = new String(b, strStart, strLen, charEncoding);
        }
        else if(streamType == StreamType.GZIP) {
            ip = new BufferedInputStream(new GZIPInputStream(ip), 2048);
            streamType = checkObjectStream(ip);
            if(streamType == StreamType.GZIP) {
                streamType = StreamType.STRING;
            }
        }
        if(o == null) {
            switch(streamType) {
              case OBJECT:
                try {
                    o = readObject(ip, serverInfo);
                } catch (ClassNotFoundException e) {
                    throw new IOException("Error Deserializing value", e);
                }
                break;
              case OPTI_OBJECT:
                try {
                    o = optiReadObject(ip, serverInfo);
                } catch (ClassNotFoundException e) {
                    throw new IOException("Error Deserializing value", e);
                }
                break;
              default:
                Reader r = new InputStreamReader(ip, charEncoding);
                int charsRead;
                char [] cbuf = new char[128];
                StringBuilder sb = new StringBuilder(len*5); // Estimating 5x compression
                while( (charsRead = r.read(cbuf)) > 0) {
                    sb.append(cbuf, 0, charsRead);
                }
                o = sb.toString();
                break;
            }
        }

        return o;
    }

    /**
     * Gets the character encoding used for Serializing Strings.
     * @return Character Encoding
     */
	public String getCharEncoding() {
		return charEncoding.name();
	}

	/**
	 * Sets the character encoding to use for Serializing Strings.
	 * @param pcharEncoding Character Encoding
	 */
	public void setCharEncoding(String pcharEncoding) {
		charEncoding = Charset.forName(pcharEncoding);
	}

	/**
	 * Gets the String Compression Threshold, -2 indicates that the AsyncRedisConnection's
	 * compressionThreshold is to be used.
	 * @return String CompressionThreshold
	 */
    public int getStringCompressionThreshold() {
        return stringCompressionThreshold;
    }

    /**
     * Sets the String Compression Threshold. -2 which is the default indicates that the AsyncRedisConnection's
     * compressionThreshold is to be used. -1 is for disabling compression and any other positiive value sets
     * the compression threshold to that value for Strings.
     * @param pstringCompressionThreshold String CompressionThreshold
     */
    public void setStringCompressionThreshold(int pstringCompressionThreshold) {
        stringCompressionThreshold = pstringCompressionThreshold;
    }

    /**
     * Gets the Object Compression Threshold. Interpretation of values is similar to the String Compression Threshold.
     * @return Object CompressionThreshold
     */
    public int getObjectCompressionThreshold() {
        return objectCompressionThreshold;
    }

    /**
     * Sets the Object Compression Threshold. Interpretation of values is similar to the String Compression Threshold.
     * @param pobjectCompressionThreshold Object CompressionThreshold
     */
    public void setObjectCompressionThreshold(int pobjectCompressionThreshold) {
        objectCompressionThreshold = pobjectCompressionThreshold;
    }

    /**
     * Checks if ptimizeObjectStorage (OPTI_JAVA_HANDLER) is enabled
     * @return true if optimizeObjectStorage is enable, false otherwise
     */
    public boolean isOptimizeObjectStorage() {
        return optimizeObjectStorage;
    }

    /**
     * Checks if {@link Number}s are to be Serialized as Java Objects. The default is false.
     * @return true if Numbers are serialized as Objects, false if they are serialized as Strings
     */
    public boolean isSerializeNumbers() {
        return serializeNumbers;
    }

    /**
     * Sets Serialization option for java Numbers.
     * @param pserializeNumbers true to serialize Numbers as Obects, false to serialize them as String
     */
    public void setSerializeNumbers(boolean pserializeNumbers) {
        serializeNumbers = pserializeNumbers;
    }

}
