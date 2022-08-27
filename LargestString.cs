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
        String? _current;
        String? _largest;
        readonly String _name;

        public LargestString(String name)
        {
            _name = name;
        }

        public void Reset()
        {
            _current = null;
            _largest = null;
        }

        public String Name
        {
            get { return _name; }
        }

        public String? Current
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

        public String? Largest
        {
            get { return _largest; }
        }
    }

}
