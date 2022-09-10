namespace WikiReader
{
    internal interface IInsertableProgress
    {
        void AddPendingRevisions(int count);

        void CompleteRevisions(int count);

        void InsertedPages(int count);

        void InsertedUsers(int count);

        void InsertedRevisions(int count);
    }
}
