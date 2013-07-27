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

import java.io.Serializable;

/**
 * Parent class of ClassDescriptors holding just the specVersion (Currently 1) and the versionNo.
 * It is separated out so that just the info can be stored in a separate key. If this is done dirty checks will
 * be faster because of the smaller size of the info Objects. However this is not done because it is an overkill.
 * @author Suresh
 *
 */
public class ClassDescriptorsInfo implements Serializable, Cloneable {

    private static final long serialVersionUID = 3813327265823406413L;

    /**
     * Spec version.
     */
    protected final int specVersion;

    /**
     * Current Version Number.
     */
    protected long versionNo;

    /**
     * Gets the Spec Version Number.
     * @return Spec Version Number
     */
    public int getSpecVersion() {
        return specVersion;
    }

    /**
     * Gets current version no. Setter is not provided. Version is automatically incremented
     * by clone method if cloned object is not writable. Starts with 0 but has to be cloned to write to
     * so that the versionNo starts with 1.
     * @return Current Version Number
     */
    public long getVersionNo() {
        return versionNo;
    }

    protected ClassDescriptorsInfo(int pspecVersion) {
        specVersion = pspecVersion;
    }

    protected ClassDescriptorsInfo() {
        this(1);
    }
}
