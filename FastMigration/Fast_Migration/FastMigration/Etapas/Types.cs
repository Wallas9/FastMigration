using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace FastMigration.Etapas
{
    public class Types
    {


        string Inicio, Fim, Server, Id, Database, Password, MySQL;
        public void Values(string inicio, string fim)
        {
            Inicio = inicio;
            Fim = fim;
        }

        public void Conn(string server, string id, string database, string password)
        {
            Server = server;
            Id = id;
            Database = database;
            Password = password;
        }

        public void Bimestre()
        {

            MySQL = $@"server = {Server}; user id = {Id}; database = {Database}; password = {Password};";

            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            try
            {
                string anoInicio = Inicio;
                string anoFim = Fim;

                MySqlCommand bimestre = new MySqlCommand();

                bimestre.Connection = conn;

                bimestre.Parameters.AddWithValue("@anoInicio", anoInicio);
                bimestre.Parameters.AddWithValue("@anoFim", anoFim);

                bimestre.CommandText = $@"SET FOREIGN_KEY_CHECKS = 0; 
            DELETE FROM etapaturma;
            -- INSERINDO AS ETAPAS BIMESTRAIS
            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '1º BIMESTRE' AS dscetapa, 1 as ordem,
            CONCAT(c.anoletivo,'-02-01') as dtinicio, CONCAT(c.anoletivo,'-04-17') as dtfinal, 10 as valoretapa, 6 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );

            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '2º BIMESTRE' AS dscetapa, 2 as ordem,
            CONCAT(c.anoletivo,'-05-01') as dtinicio, CONCAT(c.anoletivo,'-06-30') as dtfinal, 10 as valoretapa, 6 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );

            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '3º BIMESTRE' AS dscetapa, 3 as ordem,
            CONCAT(c.anoletivo,'-08-01') as dtinicio, CONCAT(c.anoletivo,'-09-30') as dtfinal, 10 as valoretapa, 6 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );

            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '4º BIMESTRE' AS dscetapa, 4 as ordem,
            CONCAT(c.anoletivo,'-10-01') as dtinicio, CONCAT(c.anoletivo,'-12-23') as dtfinal, 10 as valoretapa, 6 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );";

                bimestre.ExecuteNonQuery();
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



        public void Trimestre()
        {

            MySQL = $@"server = {Server}; user id = {Id}; database = {Database}; password = {Password};";

            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            try
            {
                string anoInicio = Inicio;
                string anoFim = Fim;

                MySqlCommand trimestre = new MySqlCommand();

                trimestre.Connection = conn;

                trimestre.Parameters.AddWithValue("@anoInicio", anoInicio);
                trimestre.Parameters.AddWithValue("@anoFim", anoFim);

                trimestre.CommandText = $@"SET FOREIGN_KEY_CHECKS = 0; 
            -- INSERINDO AS ETAPAS TRIMESTRAIS
            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '1º TRIMESTRE' AS dscetapa, 1 as ordem,
            CONCAT(c.anoletivo,'-02-05') as dtinicio, CONCAT(c.anoletivo,'-05-10') as dtfinal, 10 as valoretapa, 7 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso 
            AND c.anoletivo between @anoInicio and @anoFim );

            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '2º TRIMESTRE' AS dscetapa, 2 as ordem,
            CONCAT(c.anoletivo,'-05-14') as dtinicio, CONCAT(c.anoletivo,'-09-06') as dtfinal, 10 as valoretapa, 7 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );

            INSERT INTO etapaturma(codconfturma  ,dscetapa  ,ordem  ,dtinicio  ,dtfinal  ,valoretapa  ,mediaetapa  ,anoletivo  ,liberado  ,peso)
            (SELECT c.codconfturma, '3º TRIMESTRE' AS dscetapa, 3 as ordem,
            CONCAT(c.anoletivo,'-09-10') as dtinicio, CONCAT(c.anoletivo,'-12-11') as dtfinal, 10 as valoretapa, 7 as mediaetapa, c.anoletivo, 'N' as liberado, 1 as peso
            FROM configturma c, seriecurso sc
            where sc.codseriecurso = c.codseriecurso
            AND c.anoletivo between @anoInicio and @anoFim );";

                trimestre.ExecuteNonQuery();
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
