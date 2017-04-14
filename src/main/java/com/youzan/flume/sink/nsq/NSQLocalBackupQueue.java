package com.youzan.flume.sink.nsq;

import com.youzan.filebackup.context.BackupContext;
import com.youzan.filebackup.context.BackupScope;
import com.youzan.filebackup.context.BackupScopeBuilder;
import com.youzan.filebackup.context.DefaultBackupContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 17/4/13.
 */
public class NSQLocalBackupQueue {
    private final static Logger logger = LoggerFactory.getLogger(NSQLocalBackupQueue.class);
    private final BackupScope backupScope;
    private volatile boolean init = false;
    private AtomicLong cnt = new AtomicLong(0);
    private final Object SYNC = new Object();

    public NSQLocalBackupQueue(String backupPath, String scopeId, String contextName) {
        BackupContext context = new DefaultBackupContext(contextName);
        backupScope = BackupScopeBuilder.create(backupPath, scopeId)
                .setBackupContext(context)
                .build();
        backupScope.init();
        try {
            backupScope.openWrite();
        } catch (IOException e) {
            logger.error("Fail to open write, Path: {}, ScopeId: {}, ContextName: {}", backupPath, scopeId, contextName);
        }

        try {
            backupScope.openRead();
        } catch (IOException e) {
            logger.error("Fail to open read, Path: {}, ScopeId: {}, ContextName: {}", backupPath, scopeId, contextName);
        }
        init = true;
        logger.info("Local backup initialized.");
    }

    public boolean isInit() {
        return this.init;
    }

    public int writeBackup(byte[] messageContent) throws IOException {
        if(!isInit())
            throw new IllegalStateException("BackupQueue is not initialized for write.");
        int count = this.backupScope.tryWrite(messageContent);
        if(count > 0) {
            cnt.incrementAndGet();
            synchronized (SYNC) {
                logger.info("Try notifying one waiting read process.");
                SYNC.notify();
            }
        }
        return count;
    }

    public byte[] readBackup() throws IOException, InterruptedException {
        if(!isInit())
            throw new IllegalArgumentException("BackupQueue is not initialized for read.");
        byte[] messaege = backupScope.tryRead();
        if(null != messaege) {
            return messaege;
        } else {
            logger.info("Read process wait for write.");
            SYNC.wait();
            return readBackup();
        }
    }

    public void close() throws IOException {
        backupScope.closeWrite();
        //TODO: simply close read
        backupScope.closeRead();
    }
}
