namespace WikiLoaderEngine
{
    public interface IXmlDumpParserProgress
    {
        /// <summary>
        /// Called when progress has been made through the bytes of the file.
        /// </summary>
        /// <param name="position">Current offeset in to the file, in bytes.</param>
        /// <param name="length">Overall length of the file in bytes.</param>
        /// <param name="skipping">True when skipping through the file, false when actually loading.</param>
        public void FileProgress(long position, long length, bool skipping);

        /// <summary>
        /// Called when a page has been completely loaded.
        /// </summary>
        /// <param name="pageName">Name of page loaded.</param>
        /// <param name="usersAdded">Number of Contributor users added for this page.</param>
        /// <param name="usersExist">Number of Contributor users that already existsed on this page.</param>
        /// <param name="revisionsAdded">Number of revisions added for this page.</param>
        /// <param name="revisionsExist">Number of revisions that already existed for this page.</param>
        public void CompletedPage(string pageName, int usersAdded, int usersExist, int revisionsAdded, int revisionsExist);

        /// <summary>
        /// Called when backpressure prevents something from being enqueued.
        /// </summary>
        /// <param name="running">Number of threads running against the database.</param>
        /// <param name="queued">Number of work items queued to run.</param>
        /// <param name="pendingRevisions">Total number of pending page revisions among all queued work.</param>
        public void BackPressurePulse(long running, long queued, int pendingRevisions, IEnumerable<IWorkItemDescription> runningSet);
    }
}
