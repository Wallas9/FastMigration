using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using FirebirdSql.Data.FirebirdClient;
using System.Windows.Forms;
using FastMigration.Metodos;
using FastMigration.Etapas;

namespace FastMigration
{
    [ImportFor("EtapaTurma")]
    class ImportEtapaTurma : IImportArgs
    {
        Form2 f = new Form2();        
        public void ExecutarProcedimento(params object[] args)
        {
            f.Show();
        }


    }
}
