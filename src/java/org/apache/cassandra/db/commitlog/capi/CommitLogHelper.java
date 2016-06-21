package org.apache.cassandra.db.commitlog.capi;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;

public class CommitLogHelper {
	public static final ICommitLog instance = DatabaseDescriptor
			.getCommitLogType() == Config.CommitLogType.CAPIFlashCommitLog ? CAPIFlashCommitLog.instance
			: CommitLog.instance;
}
