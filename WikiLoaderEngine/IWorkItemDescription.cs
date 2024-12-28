namespace WikiLoaderEngine
{
    public interface IWorkItemDescription
    {
        public string ObjectName { get; }

        public string ObjectState { get; }

        public string ObjectTarget { get; }

        public int RevisionCount { get; }

        public int RemainingRevisionCount { get; }
    }
}
