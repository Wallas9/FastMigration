using System;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Windows.Forms;
using FirebirdSql.Data.FirebirdClient;
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Unidadeescolar")]
    public class ImportUnidade : IImportArgs
    {

        public void ExecutarProcedimento(params object[] args)
        {
            RecordLog r = new RecordLog();

            MySqlCommand log = new MySqlCommand();

            string MySQL = $@"server = {args[1]}; user id = {args[2]}; database = {args[0]}; password = {args[7]};";
            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            log.Connection = conn;

            string FbConn = $@"DataSource = {args[4]}; Database = {args[3]}; username = {args[5]}; password = {args[6]}; CHARSET = NONE;";
            FbConnection conn2 = new FbConnection(FbConn);
            conn2.Open();

            DataTable dtable = new DataTable();

            try
            {

                FbCommand MySelect = new FbCommand(@"select u.unidade as codunidade,
                u.nome as dscunidade,
                u.fantasia as nomefantasia,
                replace(replace(replace(replace(u.cnpj,'.',''),'-',''),'/',''),')','') as cnpj,
                u.endereco as endereco,
                u.bairro as bairro,
                c.cod_cidade as cidade,
                replace(replace(replace(replace(u.cep,'.',''),'-',''),'/',''),')','') as cep,
                replace(replace(replace(replace(replace(replace(u.telefone,'(',''),'.',''),'-',''),'/',''),')',''),' ','') as telefone,
                u.a_email as email,
                left(u.nome,10) as dscunidadeabrev
                from sigunida u
                left join cidade_tella c on c.nom_cidade = UPPER(u.cidade) and c.sgl_estado = u.estado;", conn2);

                FbDataAdapter adapter = new FbDataAdapter(MySelect);

                adapter.Fill(dtable);

                StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.Append("SET foreign_key_checks = 0;" +
                    "DELETE FROM unidadeescolar;" +
                    "DELETE FROM organizacaoseriecurso;" +
                    "DELETE FROM cursosunidade;" +

                    "INSERT INTO unidadeescolar(codunidade,dscunidade,nomefantasia,cnpj,endereco,bairro,cidade,cep,telefone,email,dscunidadeabrev) VALUES ");

                for (int i = 0; i < dtable.Rows.Count; i++)
                {
                    queryBuilder.Append($@"('{dtable.Rows[i]["codunidade"]}' , '{dtable.Rows[i]["dscunidade"]}' , '{dtable.Rows[i]["nomefantasia"]}' , '{dtable.Rows[i]["cnpj"]}' ,'{dtable.Rows[i]["endereco"]}' , '{dtable.Rows[i]["bairro"]}' , '{dtable.Rows[i]["cidade"]}' , '{dtable.Rows[i]["cep"]}' , '{dtable.Rows[i]["telefone"]}' , '{dtable.Rows[i]["email"]}' , '{dtable.Rows[i]["dscunidadeabrev"]}'), ");
                }

                queryBuilder.Remove(queryBuilder.Length - 2, 2);

                MySqlCommand query = new MySqlCommand(queryBuilder.ToString(), conn);

                query.ExecuteNonQuery();

                DataTable dtable2 = new DataTable();

                FbCommand MySelect2 = new FbCommand(@"select
                e.codigo as codunidadesiga,
                e.descricao as dscunidade,
                left(e.resumo,10) as dscunidadeabrev,
                e.descricao as nomefantasia,
                e.cnpj as cnpj,
                e.endereco as endereco
                from sigempre e;", conn2);

                FbDataAdapter adapter2 = new FbDataAdapter(MySelect2);

                adapter2.Fill(dtable2);

                StringBuilder queryBuilder2 = new StringBuilder();
                queryBuilder2.Append(@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_unidadeescolar_siga;
                CREATE TABLE mig_unidadeescolar_siga (

	            codunidadesiga          VARCHAR(100) NOT NULL,
	            dscunidade				VARCHAR(100) NOT NULL,
	            dscunidadeabrev         VARCHAR(100) NOT NULL,
	            nomefantasia            VARCHAR(100) NOT NULL,
	            cnpj					VARCHAR(100),
	            endereco                VARCHAR(100));
                
                ALTER TABLE unidadeescolar ADD COLUMN codunidadesiga INTEGER;

	            INSERT INTO mig_unidadeescolar_siga(codunidadesiga,dscunidade,dscunidadeabrev,nomefantasia,cnpj,endereco) VALUES ");

                for (int i = 0; i < dtable2.Rows.Count; i++)
                {
                    queryBuilder2.Append($@"('{dtable2.Rows[i]["codunidadesiga"]}' , '{dtable2.Rows[i]["dscunidade"]}' , '{dtable2.Rows[i]["dscunidadeabrev"]}' , '{dtable2.Rows[i]["nomefantasia"]}' ,'{dtable2.Rows[i]["cnpj"]}' , '{dtable2.Rows[i]["endereco"]}'), ");
                }

                queryBuilder2.Remove(queryBuilder2.Length - 2, 2);

                MySqlCommand query2 = new MySqlCommand(queryBuilder2.ToString(), conn);

                query2.ExecuteNonQuery();

                DataTable dtable3 = new DataTable();

                MySqlCommand MySelect3 = new MySqlCommand(@"SELECT codunidadesiga,dscunidade,dscunidadeabrev,nomefantasia,cnpj,endereco FROM mig_unidadeescolar_siga
				WHERE cnpj NOT IN (SELECT cnpj FROM unidadeescolar)
				GROUP BY cnpj;", conn);

                MySqlDataAdapter adapter3 = new MySqlDataAdapter(MySelect3);

                adapter3.Fill(dtable3);

                StringBuilder queryBuilder3 = new StringBuilder();
                queryBuilder3.Append("INSERT INTO unidadeescolar(codunidadesiga,dscunidade,dscunidadeabrev,nomefantasia,cnpj,endereco) VALUES ");

                for (int i = 0; i < dtable3.Rows.Count; i++)
                {
                    queryBuilder3.Append($@"('{dtable3.Rows[i]["codunidadesiga"]}' , '{dtable3.Rows[i]["dscunidade"]}' , '{dtable3.Rows[i]["dscunidadeabrev"]}' , '{dtable3.Rows[i]["nomefantasia"]}' ,'{dtable3.Rows[i]["cnpj"]}' , '{dtable3.Rows[i]["endereco"]}'), ");
                }

                queryBuilder3.Remove(queryBuilder3.Length - 2, 2);

                MySqlCommand query3 = new MySqlCommand(queryBuilder3.ToString(), conn);

                query3.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {
                //para remover a coluna
                log.CommandText = "ALTER TABLE unidadeescolar DROP COLUMN codunidadesiga;";
                log.ExecuteNonQuery();
                MessageBox.Show(err.Message);

                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from unidadeescolar;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}
