/*
 * Copyright (C) 2014 Suresh Mahalingam.  All rights reserved.
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

import java.util.Comparator;

import org.aredis.util.SortedArray;
import org.aredis.util.SortedArray.IndexUpdater;

/**
 * Compares two ServerInfo Objects by comparing their hosts followed by ports. It also maintains
 * a {@link SortedArray} of unique ServerInfos using the comparator.
 * @author Suresh
 *
 */
public class ServerInfoComparator implements Comparator<ServerInfo> {

    private static final SortedArray<ServerInfo> serverInfosArray = new SortedArray<ServerInfo>();

    /**
     * Single instance of the ServerInfoComparator.
     */
    public static final ServerInfoComparator INSTANCE = new ServerInfoComparator();

    private ServerInfoComparator() {
    }

    /**
     * Find a ServerInfo or creates it.
     * @param serverInfo Server Info
     * @param indexUpdater Index Updater to call in case a new entry is made
     * @return Existing ServerInfo if found else the passed ServerInfo
     */
    public static ServerInfo findItem(ServerInfo serverInfo, IndexUpdater indexUpdater) {
        return serverInfosArray.findItem(serverInfo, INSTANCE, indexUpdater);
    }

    @Override
    public int compare(ServerInfo s1, ServerInfo s2) {
        int result = 0;
        if (s1 != null) {
            result = 1;
            if (s2 != null) {
                String host1 = s1.getHost();
                String host2 = s2.getHost();
                result = 0;
                if (host1 != null) {
                    result = 1;
                    if (host2 != null) {
                        result = host1.compareTo(host2);
                    }
                } else if (host2 != null) {
                    result = -1;
                }
                if (result == 0) {
                    result = s1.getPort() - s2.getPort();
                }
            }
        } else if (s2 != null) {
            result = -1;
        }

        return result;
    }

}
