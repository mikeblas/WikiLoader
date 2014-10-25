using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    /// <summary>
    /// Represents a user; contains their name (as a string) and an ID integer.
    /// </summary>
    class User
    {
        String _userName = null;
        Int64 _userId = 0;

        /// <summary>
        /// Create a User object
        /// </summary>
        /// <param name="userId">user ID integer</param>
        /// <param name="userName">user name as a string</param>
        public User(Int64 userId, String userName)
        {
            _userId = userId;
            _userName = userName;
        }
    }
}
