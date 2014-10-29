using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    class LargestString
    {
        String _current;
        String _largest;
        String _name;

        public LargestString(String name)
        {
            _name = name;
        }

        public void test(String str)
        {
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

        public String Current
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

        public String Largest
        {
            get { return _largest; }
        }
    }

}
