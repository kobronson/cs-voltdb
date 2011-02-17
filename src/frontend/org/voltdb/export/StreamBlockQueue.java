/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.export;

import java.util.ArrayDeque;
import java.util.Iterator;

import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.VoltFile;

import org.voltdb.utils.DBBPool.BBContainer;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A customized queue for StreamBlocks that contain export data. The queue is able to
 * overflow to disk when more then two stream blocks are stored
 * as well as persist to disk when sync is invoked. Right now sync doesn't actually do an fsync on
 * the file unless it is specifically requested. It just pushed the two in memory blocks to the persistent
 *
 * portion of the queue
 *
 */
public class StreamBlockQueue {
    /**
     * Deque containing reference to stream blocks that are in memory. Some of these
     * stream blocks may still be persisted to disk others are stored completely in memory
     */
    private final ArrayDeque<StreamBlock> m_memoryDeque = new ArrayDeque<StreamBlock>();

    /**
     * A deque for persisting data to disk both for persistence and as a means of overflowing storage
     */
    private final BinaryDeque m_persistentDeque;

    public StreamBlockQueue(String path, String nonce) throws java.io.IOException {
        m_persistentDeque = new PersistentBinaryDeque( nonce, new VoltFile(path));
    }

    public boolean isEmpty() throws IOException {
        if (m_memoryDeque.isEmpty() && m_persistentDeque.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Wrapper around the common operation of pulling an element out of the persistent deque.
     * The behavior is complicated (and might change) since the persistent deque can throw an IOException.
     * The poll always removes the element from the persistent queue
     * (although not necessarily removing the file backing, that happens at deleteContents) and will add
     * a reference to the block to the in memory deque unless actuallyPoll is true.
     * @param actuallyPoll
     * @return
     */
    private StreamBlock pollPersistentDeque(boolean actuallyPoll) {
        BBContainer cont = null;
        try {
            cont = m_persistentDeque.poll();
        } catch (IOException e) {
            //TODO What is the right error handling strategy for these IOExceptions?
            throw new RuntimeException(e);
        }

        if (cont == null) {
            return null;
        } else {
            //If the container is not null, unpack it.
            final BBContainer fcont = cont;
            long uso = cont.b.getLong();
            ByteBuffer buf = cont.b.slice();
            //Pass the stream block a subset of the bytes, provide
            //a container that discards the original returned by the persistent deque
            StreamBlock block = new StreamBlock( new BBContainer(buf, 0L) {
                    @Override
                    public void discard() {
                        fcont.discard();
                    }
                },
                uso,
                true);

            //Optionally store a reference to the block in the in memory deque
            if (!actuallyPoll) {
                m_memoryDeque.offer(block);
            }
            return block;
        }
    }
    /*
     * Present an iterator that is backed by the blocks
     * that are already loaded as well as blocks that
     * haven't been polled from the persistent deque.
     *
     * The iterator wraps an iterator from the memoryDeque,
     * and regenerates it every time an element is added to the memoryDeque from
     * the persistent deque.
     */
    public Iterator<StreamBlock> iterator() {
        return new Iterator<StreamBlock>() {
            private Iterator<StreamBlock> m_memoryIterator = m_memoryDeque.iterator();
            @Override
            public boolean hasNext() {
                if (m_memoryIterator.hasNext()) {
                    return true;
                } else {
                    if (pollPersistentDeque(false) != null) {
                        m_memoryIterator = m_memoryDeque.iterator();
                        for (int ii = 0; ii < m_memoryDeque.size() - 1; ii++) {
                            m_memoryIterator.next();
                        }
                        return true;
                    }
                }
                return false;
            }

            @Override
            public StreamBlock next() {
                if (m_memoryIterator.hasNext()) {
                    return m_memoryIterator.next();
                }

                StreamBlock block = pollPersistentDeque(false);
                if (block == null) {
                    return null;
                } else {
                    m_memoryIterator = m_memoryDeque.iterator();
                    for (int ii = 0; ii < m_memoryDeque.size(); ii++) {
                        m_memoryIterator.next();
                    }
                    return block;
                }
            }

            @Override
            public void remove() {
                m_memoryIterator.remove();
            }
        };
    }

    public StreamBlock peek() {
        if (m_memoryDeque.peek() != null) {
            return m_memoryDeque.peek();
        }
        return pollPersistentDeque(false);
    }

    public StreamBlock poll() {
        StreamBlock sb = null;
        if (m_memoryDeque.peek() != null) {
            sb = m_memoryDeque.poll();
        } else {
            sb = pollPersistentDeque(true);
        }
        return sb;
    }

    public void pop() {
        if (m_memoryDeque.isEmpty()) {
            if (pollPersistentDeque(true) == null) {
                throw new java.util.NoSuchElementException();
            }
        } else {
            m_memoryDeque.pop();
        }

    }

    /*
     * Only allow two blocks in memory, put the rest in the persistent deque
     */
    public void offer(StreamBlock streamBlock) throws IOException {
        //Already have two blocks, put it in the deque
        if (m_memoryDeque.size() > 1) {
            m_persistentDeque.offer(streamBlock.asBufferChain());
        } else {
            //Don't offer into the memory deque if there is anything waiting to be
            //polled out of the persistent deque. Check the persistent deque
            if (pollPersistentDeque(false) != null) {
                //Polling the persistent deque cause there to be two blocks in memory
                if (m_memoryDeque.size() > 1) {
                    m_persistentDeque.offer(streamBlock.asBufferChain());
                //Poll persistent deque one more time, if it fails then keep this one in memory
                } else if (pollPersistentDeque(false) == null) {
                    m_memoryDeque.offer(streamBlock);
                } else {
                //Polling a second time moved a second block in memory from disk, flush this one to disk
                    m_persistentDeque.offer( streamBlock.asBufferChain());
                }
            } else {
            //Persistent deque is empty put this in memory
                m_memoryDeque.offer(streamBlock);
            }

        }
    }

    /*
     * Push all the buffers that are in memory to disk
     * and then have the persistent deque sync.
     * Skip the fsync for an asynchronous push of the in memory
     * buffers to disk
     */
    public void sync(boolean nofsync) throws IOException {
        ArrayDeque<BBContainer[]> buffersToPush = new ArrayDeque<BBContainer[]>();
        Iterator<StreamBlock> iter = m_memoryDeque.descendingIterator();
        while (iter.hasNext()) {
            StreamBlock sb = iter.next();
            if (sb.isPersisted()) {
                break;
            }
            buffersToPush.push( sb.asBufferChain() );
            iter.remove();
        }
        m_memoryDeque.clear();
        if (!buffersToPush.isEmpty()) {
            m_persistentDeque.push(buffersToPush.toArray(new BBContainer[0][0]));
        }
        if (!nofsync) {
            m_persistentDeque.sync();
        }
    }

    public long sizeInBytes() {
        long memoryBlockUsage = 0;
        for (StreamBlock b : m_memoryDeque) {
            if (b.isPersisted()) {
                break;
            }
            memoryBlockUsage += b.totalUso();
        }
        return memoryBlockUsage + m_persistentDeque.sizeInBytes();
    }
}