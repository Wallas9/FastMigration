using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    public class ImportCidade : IImportArgs
    {

        public void ExecutarProcedimento(params object[] args)
        {
            RecordLog r = new RecordLog();

            FbCommand log = new FbCommand();

            FbConnection conn2 = null;
            MySqlConnection conn = null;

            log.Connection = conn2;

            try
            {
                DataTable dtable = new DataTable();

                FbCommand create = new FbCommand($@"
                CREATE TABLE cidade_tella (
                cod_cidade INT NOT NULL,
                cod_estado SMALLINT NOT NULL,
                nom_cidade varchar(100) NOT NULL,
                municipioibge INT,
                sgl_estado VARCHAR(5),
                nom_estado VARCHAR(250),
                PRIMARY KEY (cod_cidade));", conn2);
                create.ExecuteNonQuery();

                FbCommand index = new FbCommand($@"CREATE INDEX index_nom_cidade ON cidade_tella (nom_cidade);", conn2);
                index.ExecuteNonQuery();

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append(@"SELECT c.cod_cidade, c.cod_estado, c.nom_cidade, ifnull(c.municipioibge,0) as municipioibge, e.sgl_estado, e.nom_estado FROM cidade c
                JOIN estado e ON (c.cod_estado = e.cod_estado)
                WHERE e.cod_estado NOT IN (28);");

                MySqlDataAdapter select = new MySqlDataAdapter(queryBuilder.ToString(), conn);
                select.Fill(dtable);

                StringBuilder queryBuilder2 = new StringBuilder();

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder2.Append("INSERT INTO cidade_tella (cod_cidade,cod_estado,nom_cidade,municipioibge, sgl_estado,nom_estado) VALUES ");
                    queryBuilder2.Append($@"({dtable.Rows[i]["cod_cidade"]} , {dtable.Rows[i]["cod_estado"]} , '{dtable.Rows[i]["nom_cidade"]}' , {dtable.Rows[i]["municipioibge"]} , '{dtable.Rows[i]["sgl_estado"]}' , '{dtable.Rows[i]["nom_estado"]}'); ");

                    FbCommand query = new FbCommand(queryBuilder2.ToString(), conn2);

                    query.ExecuteNonQuery();

                    queryBuilder2.Clear();
                }

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from cidade_tella;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}

