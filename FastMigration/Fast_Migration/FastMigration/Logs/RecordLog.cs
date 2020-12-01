using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FastMigration.Logs
{
    public class RecordLog
    {
        public string local = $"C:\\Users\\{Environment.UserName}\\AppData\\Roaming\\Microsoft\\Windows\\Start Menu\\Programs\\FastMigration\\FastLog.txt";
        public void Record(string tabela, int qtd)
        {
            using (StreamWriter s = new StreamWriter(local, true))
            {
                s.WriteLine("Usuário - {0}; Data - {1}; Tabela - {2}; Quantidade de registros - {3} registros foram migrados", Environment.UserName, DateTime.Now.ToString(), tabela, qtd);
                s.Close();
            }
        }
    }
}
