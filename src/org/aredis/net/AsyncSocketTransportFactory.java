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

package org.aredis.net;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * Factory class to create an {@link AsyncSocketTransport} connection to a Redis Server via the getTransport method.
 * The default implementation is to return an {@link AsyncJavaSocketTransport} object constructed via the host, port and this as the transportFactory.
 * The implementation can be changed to another class providing the same constructor. The Fully qualified className has to be specied as a
 * System property org.aredis.net.AsyncSocketTransport or as a property with the same name in aredis_transport.properties which should be in the CLASSPATH.\
 * Set the className as custom if you want to extend this class and override getTransport and set defaultFactory in the sub-class's static block so that the
 * getDefault method returns the overridden AsyncSocketTransportFactory.
 *
 * @author suresh
 *
 */
public class AsyncSocketTransportFactory {
    /**
     * Resolved constructor to use to create the AsyncSocketTransport. This is initialized to the statically
     * resolved constructor RESOLVED_TRANSPORT_CONSTRUCTOR.
     */
    protected Constructor<AsyncSocketTransport> transportConstructor;

    /**
     * Default factory which is returned by the singleton method getDefault.
     */
    protected static volatile AsyncSocketTransportFactory defaultFactory;

    /**
     * Default class name of AsyncJavaSocketTransport that is used if no custom class is specified.
     */
    public static final String DEFAULT_CLASS_NAME = "org.aredis.net.AsyncJavaSocketTransport";

    /**
     * Used this as the class name you are extending AsyncSocketTransportFactory and overriding getTransport.
     */
    public static final String CUSTOM_CLASS_NAME = "custom";

    /**
     * The resolved Class name created in the static block.
     */
    public static final String RESOLVED_CLASS_NAME;

    /**
     * The Resolved Consctructor that is used by getTransport.
     */
    public static final Constructor<AsyncSocketTransport> RESOLVED_TRANSPORT_CONSTRUCTOR;

    private AsyncSocketTransportConfig config;

    static {
        String className = System.getProperty("org.aredis.net.AsyncSocketTransport");
        if(className == null) {
            InputStream ip = AsyncSocketTransportFactory.class.getResourceAsStream("aredis_transport.properties");
            if(ip != null) {
                Properties p = new Properties();
                try {
                    p.load(ip);
                } catch (IOException e) {
                    String resourcePath = null;
                    try {
                        resourcePath = AsyncSocketTransportFactory.class.getResource("aredis_transport.properties").toExternalForm();
                    }
                    catch(Exception e1) {
                    }
                    throw new RuntimeException("Error loading aredis.properties from " + resourcePath, e);
                }
                finally {
                    try {
                        ip.close();
                    }
                    catch(Exception e) {
                    }
                }
                className = p.getProperty("org.aredis.net.AsyncSocketTransport");
            }
        }
        if(className == null) {
            className = DEFAULT_CLASS_NAME;
        }
        else if(CUSTOM_CLASS_NAME.equals(className)) {
            className = CUSTOM_CLASS_NAME; // To make comparisons faster
        }
        RESOLVED_CLASS_NAME = className;

        Class<AsyncSocketTransport> cls = null;
        try {
            if(!CUSTOM_CLASS_NAME.equals(className)) {
                cls = (Class<AsyncSocketTransport>) Class.forName(className);
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to load configured AsyncSocketTransport Class: " + className + ". Do you have the appropriate transport jar file in Class Path. If you have your own implementation configure it as custom?", e);
        }

        Constructor<AsyncSocketTransport> constructor = null;
        if(cls != null) {
            try {
                constructor = cls.getConstructor(String.class, int.class, AsyncSocketTransportFactory.class);
            } catch (Exception e) {
                throw new RuntimeException("Unable to find public Constructor with params(String host, int port, AsyncSocketTransportFactory factory) for AsyncSocketTransport provider: " + className, e);
            }
        }

        RESOLVED_TRANSPORT_CONSTRUCTOR = constructor;
    }

    /**
     * Create an AsyncSocketTransportFactory.
     * @param pconfig Config to use
     */
    public AsyncSocketTransportFactory(AsyncSocketTransportConfig pconfig) {
        config = pconfig;
        transportConstructor = RESOLVED_TRANSPORT_CONSTRUCTOR;
    }

    /**
     * Create an AsyncSocketTransportFactory with default config.
     */
    public AsyncSocketTransportFactory() {
        this(new AsyncSocketTransportConfig());
    }

    /**
     * Creates an AsyncSocketTransport to a redis server by using resolved transportConstructor.
     * @param host Server Host
     * @param port Server Port
     * @return New AsyncSocketTransport
     */
    public AsyncSocketTransport getTransport(String host, int port) {
        if(CUSTOM_CLASS_NAME.equals(RESOLVED_CLASS_NAME)) {
            throw new IllegalStateException("AsyncSocketTransportFactory.getTransport cannot be called after configuring org.aredis.net.AsyncSocketTransport as custom");
        }
        AsyncSocketTransport transport = null;
        try {
            transport = transportConstructor.newInstance(host, port, this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return transport;
    }

    /**
     * Singleton method to return defaultFactory.
     * @return Returns the default Transport Factory
     */
    public static AsyncSocketTransportFactory getDefault() {
        if(defaultFactory == null) {
            synchronized(AsyncSocketTransportFactory.class) {
                if(defaultFactory == null) {
                    defaultFactory = new AsyncSocketTransportFactory();
                }
            }
        }

        return defaultFactory;
    }

    /**
     * Gets Config in use.
     * @return config in use
     */
    public AsyncSocketTransportConfig getConfig() {
        return config;
    }
}
