using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace FastMigration.Etapas
{
    public class Daily 
    {
        decimal Et1, Et2;
        string Server, Id, Database, Password;
        string MySQL;
        public void Etapas(decimal et1, decimal et2)
        {
            Et1 = et1;
            Et2 = et2;
        }

        public void Conn(string server, string id, string database, string password)
        {
            Server = server;
            Id = id;
            Database = database;
            Password = password;
        }

        public void DailyMigration()
        {
            MySQL = $@"server = {Server}; user id = {Id}; database = {Database}; password = {Password};";

            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            try
            {
                MySqlCommand insert = new MySqlCommand();
                insert.Connection = conn;

                for (decimal i = Et2; i >= Et1; i--)
                {
                    insert.CommandText = $@"SET FOREIGN_KEY_CHECKS = 0; 
                    DELETE FROM notas;
                    DELETE FROM chamadaaula;
                    insert into notas  (codserie,codturma,codmateria,matricula,ano,codetapa,codprofturma)
                    (select a.CODSERIE,a.CODTURMA,a.CODMATERIA,a.MATRICULA,a.ANO,{i};a.codprofturma from acumulado a where a.ANO = year(curdate()));

                    insert into chamadaaula  (codserie,codturma,codmateria,matricula,codano,codetapa,codprofturma)
                    (select a.CODSERIE,a.CODTURMA,a.CODMATERIA,a.MATRICULA,a.ANO,{i};a.codprofturma from acumulado a where a.ANO = year(curdate()));";
                    insert.ExecuteNonQuery();
                }

            }
            catch (Exception err)
            {
                MessageBox.Show(err.Message);
            }
            finally
            {
                conn.Close();
            }
        }
    }

}





