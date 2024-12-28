namespace WikiLoaderEngine
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// manages a named string property, recording the longest value of the string ever known.
    /// </summary>
    internal class LargestString
    {
        private readonly string name;
        private string? current;
        private string? largest;

        public LargestString(string name)
        {
            this.name = name;
        }

        public string Name
        {
            get { return this.name; }
        }

        public string? Current
        {
            get
            {
                return this.current;
            }

            set
            {
                if (value != null)
                {
                    if (this.largest == null || value.Length > this.largest.Length)
                    {
                        this.largest = value;
                    }
                }

                this.current = value;
            }
        }

        public string? Largest
        {
            get { return this.largest; }
        }

        public int LargestLength
        {
            get
            {
                return this.largest == null ? 0 : this.largest.Length;
            }
        }

        public void Reset()
        {
            this.current = null;
            this.largest = null;
        }
    }
}
