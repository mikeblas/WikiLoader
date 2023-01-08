namespace WikiLoaderEngine
{
    public interface IWorkItemDescription
    {
        public string ObjectName { get; }

        public int RevisionCount { get; }

        public int RemainingRevisionCount { get; }
    }
}
