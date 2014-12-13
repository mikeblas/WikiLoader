using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WikiReader
{
    interface InsertableProgress
    {
        void AddPendingRevisions(int count);
        void CompleteRevisions(int count);

        void InsertedPages(int count);
        void InsertedUsers(int count);
        void InsertedRevisions(int Count);
    }
}
