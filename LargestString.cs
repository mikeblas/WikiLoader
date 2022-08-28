using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// manages a named string property, recording the longest value of the string ever known.
    /// </summary>
    class LargestString
    {
        string? _current;
        string? _largest;
        readonly string _name;

        public LargestString(string name)
        {
            _name = name;
        }

        public void Reset()
        {
            _current = null;
            _largest = null;
        }

        public string Name
        {
            get { return _name; }
        }

        public string? Current
        {
            get { return _current; }
            set
            {
                if (value != null)
                {
                    if (_largest == null || value.Length > _largest.Length)
                    {
                        _largest = value;
                    }
                }
                _current = value;
            }
        }

        public string? Largest
        {
            get { return _largest; }
        }

        public int LargestLength
        {
            get { if (_largest == null) return 0; return _largest.Length; } 
        }
    }
}
