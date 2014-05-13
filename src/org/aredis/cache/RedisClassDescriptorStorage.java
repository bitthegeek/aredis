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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.io.ClassDescriptors;
import org.aredis.io.ClassDescriptorStorage;

/**
 * <p>
 * Implementation of ClassDescriptorStorage used by aredis that stores the ClassDescriptors on a redis server.
 * The data is serialized using java serialization and saved under the specified key. The default key is
 * JAVA_CL_DESCRIPTORS. The key is non-volatile.
 * </p>
 *
 * <p>
 * Note that aredis uses this class via {@link PerConnectionRedisClassDescriptorStorageFactory} which is the default
 * or {@link RedisClassDescriptorStorageFactory}.
 * </p>
 *
 * <p>
 * The updateClassDescriptors method uses WATCH-MULTI-EXEC commands along with checking of versionNo to avoid dirty
 * update.
 * </p>
 *
 * @author Suresh
 *
 */
public class RedisClassDescriptorStorage extends ClassDescriptorStorage {

    private static final Log log = LogFactory.getLog(RedisClassDescriptorStorage.class);

    /**
     * Constant for the default key in which to store the Class Descriptors.
     */
    public static final String DEFAULT_DESCRIPTORS_KEY = "JAVA_CL_DESCRIPTORS";

    private AsyncRedisConnection aredis;

    private String descriptorsKey;

    private int dbIndex;

    private volatile ClassDescriptors descriptors;

    /**
     * Creates a storage for the given Redis Connection and descriptors key. The type of the connection must be
     * STANDALONE or SHARED.
     *
     * @param paredis Async Redis Connection to Use
     * @param pdescriptorsKey Key in which to store the Descriptors
     * @param pdbIndex The dbIndex to use. This could be different from that in aredis
     */
    public RedisClassDescriptorStorage(AsyncRedisConnection paredis, String pdescriptorsKey, int pdbIndex) {
        aredis = paredis;
        aredis.setDataHandler(AsyncRedisConnection.JAVA_HANDLER);
        if(pdescriptorsKey == null) {
            pdescriptorsKey = DEFAULT_DESCRIPTORS_KEY;
        }
        descriptorsKey = pdescriptorsKey;
        dbIndex = pdbIndex;
    }

    /**
     * Creates a storage for the given Redis Connection with the defalt key "JAVA_CL_DESCRIPTORS".
     * The type of the connection must be STANDALONE or SHARED. The dbIndex of the passed redis connection
     * is the dbIndex used.
     *
     * @param paredis Async Redis Connection to Use
     */
    public RedisClassDescriptorStorage(AsyncRedisConnection paredis) {
        this(paredis, DEFAULT_DESCRIPTORS_KEY, paredis.getDbIndex());
    }

    @Override
    public ClassDescriptors getMasterClassDescriptors(boolean refreshFromStore) throws IOException {
        ClassDescriptors masterDescriptors = descriptors;
        if(masterDescriptors == null || refreshFromStore) {
            boolean requiresDbChange = false;
            synchronized(aredis) {
                if(descriptors == masterDescriptors) {
                    RedisCommandInfo commandInfo = null;
                    try {
                        if(dbIndex != aredis.getDbIndex()) {
                            requiresDbChange = true;
                            aredis.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, dbIndex));
                        }
                        commandInfo = aredis.submitCommand(new RedisCommandInfo(RedisCommand.GET, descriptorsKey)).get();
                    } catch (Exception e) {
                        throw new IOException(e);
                    } finally {
                        if (requiresDbChange) {
                            aredis.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, aredis.getDbIndex()));
                        }
                    }
                    masterDescriptors = (ClassDescriptors) commandInfo.getResult();
                    if(commandInfo.getRunStatus() != CommandStatus.SUCCESS) {
                        throw new IOException("Error Running Redis GET command " + commandInfo.getRunStatus(), commandInfo.getError());
                    }
                    if(masterDescriptors == null) {
                        masterDescriptors = new ClassDescriptors();
                    }
                    if(descriptors != null) {
                        descriptors.copyMetaInfoTo(masterDescriptors);
                    }
                    descriptors = masterDescriptors;
                }
                else {
                    masterDescriptors = descriptors;
                }
            }
        }

        return masterDescriptors;
    }

    @Override
    public boolean updateClassDescriptors(ClassDescriptors updatedDescriptors) throws IOException {
        boolean success = false;
        ClassDescriptors masterDescriptors = descriptors;
        long currentVersionNo = masterDescriptors.getVersionNo();
        if(currentVersionNo + 1 < updatedDescriptors.getVersionNo()) {
            throw new IllegalArgumentException("master Version No: " + currentVersionNo + " < updated Version No: " + updatedDescriptors.getVersionNo() + " - 1");
        }
        if(currentVersionNo + 1 == updatedDescriptors.getVersionNo()) {
            synchronized(aredis) {
                if(currentVersionNo == descriptors.getVersionNo()) {
                    boolean requiresUnWatch = true;
                    boolean requiresDbChange = false;
                    try {
                        RedisCommandInfo[] commandInfos;
                        try {
                            if(dbIndex != aredis.getDbIndex()) {
                                requiresDbChange = true;
                                aredis.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, dbIndex));
                            }
                            commandInfos = aredis.submitCommands(new RedisCommandInfo[] {
                                  new RedisCommandInfo(RedisCommand.WATCH, descriptorsKey),
                                  new RedisCommandInfo(RedisCommand.GET, descriptorsKey)
                            }).get();
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                        ClassDescriptors latestMaster = (ClassDescriptors) commandInfos[1].getResult();
                        if(commandInfos[1].getRunStatus() != CommandStatus.SUCCESS) {
                            throw new IOException("Error getting master descriptor " + commandInfos[1].getRunStatus(), commandInfos[1].getError());
                        }
                        long repoVersionNo = 0;
                        if(latestMaster != null) {
                            repoVersionNo = latestMaster.getVersionNo();
                        }
                        if(currentVersionNo == repoVersionNo) {
                            requiresUnWatch = false;
                            try {
                                commandInfos = aredis.submitCommands(new RedisCommandInfo[] {
                                        new RedisCommandInfo(RedisCommand.MULTI),
                                        new RedisCommandInfo(RedisCommand.SET, descriptorsKey, updatedDescriptors),
                                        new RedisCommandInfo(RedisCommand.EXEC),
                                        new RedisCommandInfo(RedisCommand.GET, descriptorsKey)
                                  }).get();
                            } catch (Exception e) {
                                throw new IOException(e);
                            }
                            if(commandInfos[2].getRunStatus() != CommandStatus.SUCCESS) {
                                throw new IOException("Error updating master descriptor " + commandInfos[3].getRunStatus(), commandInfos[3].getError());
                            }
                            else {
                                if(commandInfos[2].getResult() != null) {
                                    success = true;
                                }
                                masterDescriptors = (ClassDescriptors) commandInfos[3].getResult();
                                if(masterDescriptors != null) {
                                    descriptors.copyMetaInfoTo(masterDescriptors);
                                    descriptors = masterDescriptors;
                                }
                            }
                        }
                        else if(latestMaster != null) {
                            descriptors.copyMetaInfoTo(latestMaster);
                            descriptors = latestMaster;
                        }
                    }
                    finally {
                        try {
                            if(requiresUnWatch) {
                                aredis.submitCommand(new RedisCommandInfo(RedisCommand.UNWATCH));
                            }
                            if (requiresDbChange) {
                                aredis.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, aredis.getDbIndex()));
                            }
                        }
                        catch(Exception e) {
                        }
                    }
                }
            }
        }
        return success ;
    }

}
