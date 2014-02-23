package org.aredis.net;

import org.aredis.util.SortedArray;

public final class ServerIndexes {

    private static final SortedArray<ServerInfo> serverInfosArray = new SortedArray<ServerInfo>();

    private ServerIndexes() {
    }

    public static int getServerInfoIndex(ServerInfo serverInfo) {
        ServerInfo [] s = new ServerInfo[1];
        s[0] = serverInfo;
        int index = serverInfosArray.getIndex(s, ServerInfoComparator.INSTANCE);
        if (index < 0) {
            index = s[0].getServerIndex();
        }

        return index;
    }
}
