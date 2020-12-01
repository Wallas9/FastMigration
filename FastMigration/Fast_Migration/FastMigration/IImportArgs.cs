using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace FastMigration
{
    interface IImportArgs
    {
        void ExecutarProcedimento(params object[] args);

    }
}
