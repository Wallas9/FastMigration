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
using FastMigration.Logs;

namespace FastMigration
{
    [ImportFor("Acumulado")]
    public class ImportAcumulado : IImportArgs
    {
        public void ExecutarProcedimento(params object[] args)
        {
            RecordLog r = new RecordLog();

            MySqlCommand log = new MySqlCommand();

            int anoMenor = 0;
            int anoMaior = 0;

            string MySQL = $@"server = {args[1]}; user id = {args[2]}; database = {args[0]}; password = {args[7]};";
            MySqlConnection conn = new MySqlConnection(MySQL);
            conn.Open();

            log.Connection = conn;

            string FbConn = $@"DataSource = {args[4]}; Database = {args[3]}; username = {args[5]}; password = {args[6]}; CHARSET = NONE;";
            FbConnection conn2 = new FbConnection(FbConn);
            conn2.Open();

            DataTable dtable = new DataTable();
            MySqlCommand insert = new MySqlCommand("", conn);
            
            insert.CommandTimeout = 86400;

            try
            {

                MySqlCommand create = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS mig_acumulado_siga;
                DELETE FROM acumulado;
                DELETE FROM acumuladoagrupado;
                DELETE FROM turmadiario;
                CREATE TABLE `mig_acumulado_siga` (
	            `CODACUMULADO` INT(11) NOT NULL AUTO_INCREMENT,
	            `MATRICULA` varchar(100) NOT NULL,
	            `CODMATERIA` varchar(100) NOT NULL,
	            `ANO` varchar(100) NOT NULL,
	            `CODSERIE` varchar(100) NOT NULL,
	            `CODTURMA` varchar(100) NOT NULL,
	            `MEDIA1` varchar(100) NULL DEFAULT NULL,
	            `FALTAS1` varchar(100) NULL DEFAULT NULL,
	            `MEDIA2` varchar(100) NULL DEFAULT NULL,
	            `FALTAS2` varchar(100) NULL DEFAULT NULL,
	            `MEDIA3` varchar(100) NULL DEFAULT NULL,
	            `FALTAS3` varchar(100) NULL DEFAULT NULL,
	            `MEDIA4` varchar(100) NULL DEFAULT NULL,
	            `FALTAS4` varchar(100) NULL DEFAULT NULL,
	            `RECSEMESTRE1` varchar(100) NULL DEFAULT NULL,
	            `RECSEMESTRE2` varchar(100) NULL DEFAULT NULL,
	            `RECFINAL`  varchar(100) NULL DEFAULT NULL,
	            `RECFINAL2` varchar(100) NULL DEFAULT NULL,
	            `RECFINAL3` varchar(100) NULL DEFAULT NULL,
	            `alterado` CHAR(1) NULL DEFAULT NULL,
	            `MEDSEM1` varchar(100) NULL DEFAULT NULL,
	            `MEDSEM2` varchar(100) NULL DEFAULT NULL,
	            `RESULTFINAL` varchar(100) NULL DEFAULT NULL,
	            `TOTFALTAS` varchar(100) NULL DEFAULT NULL,
	            `SITUACAO` CHAR(1) NULL DEFAULT NULL,
	            `codprofturma` varchar(100) NULL DEFAULT NULL,
	            `rec1`  varchar(100) NULL DEFAULT NULL,
	            `rec2`  varchar(100) NULL DEFAULT NULL,
	            `rec3`  varchar(100) NULL DEFAULT NULL,
	            `rec4`  varchar(100) NULL DEFAULT NULL,
	            `soma1` varchar(100) NULL DEFAULT NULL,
	            `soma2` varchar(100) NULL DEFAULT NULL,
	            `soma3` varchar(100) NULL DEFAULT NULL,
	            `soma4` varchar(100) NULL DEFAULT NULL,
	            `somasemestre1` varchar(100) NULL DEFAULT NULL,
	            `somasemestre2` varchar(100) NULL DEFAULT NULL,
	            `CODPROFESSOR` varchar(100) NULL DEFAULT NULL,
	            `usuario` varchar(100) NULL DEFAULT NULL,
	            `codcalculoacumulado` varchar(100) NULL DEFAULT NULL,
	            `dtvalidacaocalculo`  varchar(100) NULL DEFAULT NULL,
	            `somaetapas` varchar(100) NULL DEFAULT NULL,
	            `mig_conceito1` VARCHAR(5) NULL DEFAULT NULL,
	            `mig_conceito2` VARCHAR(5) NULL DEFAULT NULL,
	            `mig_conceito3` VARCHAR(5) NULL DEFAULT NULL,
	            `mig_conceito4` VARCHAR(5) NULL DEFAULT NULL,
	            `mig_conceitofinal` VARCHAR(5) NULL DEFAULT NULL,
	            `mig_pontosaprovconselho` varchar(100) NULL DEFAULT NULL,
	            PRIMARY KEY (`CODACUMULADO`) USING BTREE
                )
                COLLATE='latin1_swedish_ci'
                ENGINE=InnoDB
                ;", conn);
                create.ExecuteNonQuery();

                ResetaValorAcumulado.Resetar(out anoMenor, out anoMaior, FbConn);

                while (anoMenor <= anoMaior)
                {
                    dtable.Clear();

                    FbCommand select = new FbCommand($@"select
                -- d.descricao,
                m.codigo as MATRICULA,
                a.disciplina as CODMATERIA,
                a.anoletivo as ANO,
                (m.unidade || m.curso || m.serie) as CODSERIE,
                tt.codturma as CODTURMA,
                iif(a.resultbim1 in ('A','B','C','D','E','DISP'),NULL, a.resultbim1) as MEDIA1,
                a.faltabim1 as FALTAS1,
                iif(a.resultbim2 in ('A','B','C','D','E','DISP'),NULL, a.resultbim2) as MEDIA2,
                a.faltabim2 as FALTAS2,
                iif(a.resultbim3 in ('A','B','C','D','E','DISP'),NULL, a.resultbim3) as MEDIA3,
                a.faltabim3 as FALTAS3,
                iif(a.resultbim4 in ('A','B','C','D','E','DISP'),NULL, a.resultbim4) as MEDIA4,
                a.faltabim4 as FALTAS4,
                iif(a.notarecup in ('A','B','C','D','E','DISP'),NULL, a.notarecup) as RECFINAL,
                iif(a.mediafinal in ('A','B','C','D','E','DISP'),NULL, a.mediafinal) as RESULTFINAL,
                (coalesce(a.faltabim1,0) + coalesce(a.faltabim2,0) + coalesce(a.faltabim3,0) + coalesce(a.faltabim4,0)) as TOTFALTAS,
                iif(a.recupbim1 in ('A','B','C','D','E','DISP'),NULL, a.recupbim1) as rec1,
                iif(a.recupbim2 in ('A','B','C','D','E','DISP'),NULL, a.recupbim2) as rec2,
                iif(a.recupbim3 in ('A','B','C','D','E','DISP'),NULL, a.recupbim3) as rec3,
                iif(a.recupbim4 in ('A','B','C','D','E','DISP'),NULL, a.recupbim4) as rec4,
                iif(a.mediabim1 in ('A','B','C','D','E','DISP'),NULL, a.mediabim1) as soma1,
                iif(a.mediabim2 in ('A','B','C','D','E','DISP'),NULL, a.mediabim2) as soma2,
                iif(a.mediabim3 in ('A','B','C','D','E','DISP'),NULL, a.mediabim3) as soma3,
                iif(a.mediabim4 in ('A','B','C','D','E','DISP'),NULL, a.mediabim4) as soma4,
                iif(a.mediabim in ('A','B','C','D','E','DISP'),NULL, a.mediabim) as somaetapas,
                iif(a.resultbim1 in ('A','B','C','D','E'),a.resultbim1,NULL ) as mig_conceito1,
                iif(a.resultbim2 in ('A','B','C','D','E'),a.resultbim2,NULL ) as mig_conceito2,
                iif(a.resultbim3 in ('A','B','C','D','E'),a.resultbim3,NULL ) as mig_conceito3,
                iif(a.resultbim4 in ('A','B','C','D','E'),a.resultbim4,NULL ) as mig_conceito4,
                iif(a.mediafinal in ('A','B','C','D','E'),a.mediafinal,NULL ) as mig_conceitofinal,
                iif(a.notaconse in ('A','B','C','D','E','DISP'),NULL, a.notaconse) as mig_pontosaprovconselho --  ,
                -- a.*
                from signotfa a
                join sigaluno m on (a.unidade = m.unidade and a.anoletivo = m.anoletivo and a.periodo = m.periodo and a.curso = m.curso and a.serie = m.serie and a.classe = m.classe and a.turno = m.turno and a.chamada = m.chamada )
                join sigclass t on (a.unidade = t.unidade and a.anoletivo = t.anoletivo and a.periodo = t.periodo and a.curso = t.curso and a.serie = t.serie and a.classe = t.classe and a.turno = t.turno)
                join turma_tella tt on tt.dscturma = (COALESCE(t.classe,'X') || COALESCE(t.turno,'Y') || COALESCE(t.periodo,'Z'))
                join sigdisci d on (d.codigo = a.disciplina)
                where a.anoletivo = {anoMenor} and a.resultbim1 is null", conn2);

                    FbDataAdapter adapter = new FbDataAdapter(select);

                    adapter.Fill(dtable);

                    StringBuilder queryBuilder = new StringBuilder();
                    queryBuilder.Append($@"INSERT INTO mig_acumulado_siga (MATRICULA, CODMATERIA, ANO, CODSERIE, CODTURMA, MEDIA1, FALTAS1, MEDIA2, FALTAS2, MEDIA3, FALTAS3, MEDIA4, FALTAS4, RECFINAL, RESULTFINAL, TOTFALTAS, rec1, rec2, rec3, rec4, soma1, soma2, soma3, soma4, somaetapas, mig_conceito1, mig_conceito2, mig_conceito3, mig_conceito4, mig_conceitofinal, mig_pontosaprovconselho ) VALUES ");

                    try
                    {

                        for (int i = 0; i < dtable.Rows.Count; i++)
                        {

                            queryBuilder.Append($@"('{dtable.Rows[i]["MATRICULA"]}' , '{dtable.Rows[i]["CODMATERIA"]}' , '{dtable.Rows[i]["ANO"]}' , '{dtable.Rows[i]["CODSERIE"]}' , '{dtable.Rows[i]["CODTURMA"]}' , '{dtable.Rows[i]["MEDIA1"]}' , '{dtable.Rows[i]["FALTAS1"]}' , '{dtable.Rows[i]["MEDIA2"]}' , '{dtable.Rows[i]["FALTAS2"]}' ,'{dtable.Rows[i]["MEDIA3"]}' , '{dtable.Rows[i]["FALTAS3"]}' , '{dtable.Rows[i]["MEDIA4"]}' , '{dtable.Rows[i]["FALTAS4"]}' , '{dtable.Rows[i]["RECFINAL"]}' , '{dtable.Rows[i]["RESULTFINAL"]}' , '{dtable.Rows[i]["TOTFALTAS"]}' , '{dtable.Rows[i]["rec1"]}' , '{dtable.Rows[i]["rec2"]}' , '{dtable.Rows[i]["rec3"]}' , '{dtable.Rows[i]["rec4"]}' , '{dtable.Rows[i]["soma1"]}' , '{dtable.Rows[i]["soma2"]}' , '{dtable.Rows[i]["soma3"]}' , '{dtable.Rows[i]["soma4"]}' , '{dtable.Rows[i]["somaetapas"]}' , '{dtable.Rows[i]["mig_conceito1"]}' , '{dtable.Rows[i]["mig_conceito2"]}' , '{dtable.Rows[i]["mig_conceito3"]}' , '{dtable.Rows[i]["mig_conceito4"]}' , '{dtable.Rows[i]["mig_conceitofinal"]}' , '{dtable.Rows[i]["mig_pontosaprovconselho"]}'), ");

                        }

                        queryBuilder.Remove(queryBuilder.Length - 2, 2);
                        insert = new MySqlCommand(queryBuilder.ToString(), conn);
                        insert.ExecuteNonQuery();
                    }
                    catch (Exception err)
                    {
                        if (err.Message == "Packets larger than max_allowed_packet are not allowed.")
                        {
                            for (int i = 0; i < dtable.Rows.Count; i++)
                            {
                                insert.CommandText = $@"INSERT INTO mig_acumulado_siga (MATRICULA, CODMATERIA, ANO, CODSERIE, CODTURMA, MEDIA1, FALTAS1, MEDIA2, FALTAS2, MEDIA3, FALTAS3, MEDIA4, FALTAS4, RECFINAL, RESULTFINAL, TOTFALTAS, rec1, rec2, rec3, rec4, soma1, soma2, soma3, soma4, somaetapas, mig_conceito1, mig_conceito2, mig_conceito3, mig_conceito4, mig_conceitofinal, mig_pontosaprovconselho ) VALUES  
                                                     ('{dtable.Rows[i]["MATRICULA"]}' , '{dtable.Rows[i]["CODMATERIA"]}' , '{dtable.Rows[i]["ANO"]}' , '{dtable.Rows[i]["CODSERIE"]}' , '{dtable.Rows[i]["CODTURMA"]}' , '{dtable.Rows[i]["MEDIA1"]}' , '{dtable.Rows[i]["FALTAS1"]}' , '{dtable.Rows[i]["MEDIA2"]}' , '{dtable.Rows[i]["FALTAS2"]}' ,'{dtable.Rows[i]["MEDIA3"]}' , '{dtable.Rows[i]["FALTAS3"]}' , '{dtable.Rows[i]["MEDIA4"]}' , '{dtable.Rows[i]["FALTAS4"]}' , '{dtable.Rows[i]["RECFINAL"]}' , '{dtable.Rows[i]["RESULTFINAL"]}' , '{dtable.Rows[i]["TOTFALTAS"]}' , '{dtable.Rows[i]["rec1"]}' , '{dtable.Rows[i]["rec2"]}' , '{dtable.Rows[i]["rec3"]}' , '{dtable.Rows[i]["rec4"]}' , '{dtable.Rows[i]["soma1"]}' , '{dtable.Rows[i]["soma2"]}' , '{dtable.Rows[i]["soma3"]}' , '{dtable.Rows[i]["soma4"]}' , '{dtable.Rows[i]["somaetapas"]}' , '{dtable.Rows[i]["mig_conceito1"]}' , '{dtable.Rows[i]["mig_conceito2"]}' , '{dtable.Rows[i]["mig_conceito3"]}' , '{dtable.Rows[i]["mig_conceito4"]}' , '{dtable.Rows[i]["mig_conceitofinal"]}' , '{dtable.Rows[i]["mig_pontosaprovconselho"]}');";
                                insert.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            MessageBox.Show(err.Message);
                            conn2.Close();
                            conn.Close();
                        }
                    }

                    anoMenor++;
                }

                MySqlCommand update = new MySqlCommand(@$"
                UPDATE mig_acumulado_siga set matricula = NULL where matricula = '';
                update mig_acumulado_siga set codmateria = null where codmateria = '';
                update mig_acumulado_siga set ano = null where ano = '';
                update mig_acumulado_siga set codserie = null where codserie = '';
                update mig_acumulado_siga set codturma = null where codturma = '';
                UPDATE mig_acumulado_siga set media1 = NULL where media1 = '';
                UPDATE mig_acumulado_siga set faltas1 = NULL where faltas1 = '';
                update mig_acumulado_siga set media2 = null where media2 = '';
                UPDATE mig_acumulado_siga set faltas2 = NULL where faltas2 = '';
                update mig_acumulado_siga set media3 = null where media3 = '';
                update mig_acumulado_siga set faltas3 = null where faltas3 = '';
                update mig_acumulado_siga set media4 = null where media4 = '';
                UPDATE mig_acumulado_siga set faltas4 = NULL where faltas4 = '';
                UPDATE mig_acumulado_siga set recfinal = NULL where recfinal = '';
                update mig_acumulado_siga set resultfinal = null where resultfinal = '';
                update mig_acumulado_siga set totfaltas = null where totfaltas = '';
                update mig_acumulado_siga set rec1 = null where rec1 = '';
                update mig_acumulado_siga set rec2 = null where rec2 = '';
                UPDATE mig_acumulado_siga set rec3 = NULL where rec3 = '';
                UPDATE mig_acumulado_siga set rec4 = NULL where rec4 = '';
                update mig_acumulado_siga set soma1 = null where soma1 = '';
                update mig_acumulado_siga set soma2 = null where soma2 = '';
                update mig_acumulado_siga set soma3 = null where soma3 = '';
                update mig_acumulado_siga set soma4 = null where soma4 = '';
                UPDATE mig_acumulado_siga set somaetapas = NULL where somaetapas = '';
                UPDATE mig_acumulado_siga set mig_conceito1 = NULL where mig_conceito1 = '';
                update mig_acumulado_siga set mig_conceito2 = null where mig_conceito2 = '';
                update mig_acumulado_siga set mig_conceito3 = null where mig_conceito3 = '';
                update mig_acumulado_siga set mig_conceito4 = null where mig_conceito4 = '';
                update mig_acumulado_siga set mig_conceitofinal = null where mig_conceitofinal = '';
                update mig_acumulado_siga set mig_pontosaprovconselho = null where mig_pontosaprovconselho = '' or mig_pontosaprovconselho REGEXP '^-?[A-Z]+$';", conn);
                update.ExecuteNonQuery();

                MySqlCommand acumulado = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; 
                INSERT INTO acumulado (CODACUMULADO, MATRICULA, CODMATERIA, ANO, CODSERIE, CODTURMA, MEDIA1, FALTAS1, MEDIA2, FALTAS2, MEDIA3, FALTAS3, MEDIA4, FALTAS4, RECSEMESTRE1, RECSEMESTRE2, RECFINAL, RECFINAL2, RECFINAL3, alterado, MEDSEM1, MEDSEM2, RESULTFINAL, TOTFALTAS, SITUACAO, codprofturma, rec1, rec2, rec3, rec4, soma1, soma2, soma3, soma4, somasemestre1, somasemestre2, CODPROFESSOR, usuario, codcalculoacumulado, dtvalidacaocalculo, somaetapas )
                (SELECT CODACUMULADO, MATRICULA, CODMATERIA, ANO, CODSERIE, CODTURMA, MEDIA1, FALTAS1, MEDIA2, FALTAS2, MEDIA3, FALTAS3, MEDIA4, FALTAS4, RECSEMESTRE1, RECSEMESTRE2, RECFINAL, RECFINAL2, RECFINAL3, alterado, MEDSEM1, MEDSEM2, RESULTFINAL, TOTFALTAS, SITUACAO, codprofturma, rec1, rec2, rec3, rec4, soma1, soma2, soma3, soma4, somasemestre1, somasemestre2, CODPROFESSOR, usuario, codcalculoacumulado, dtvalidacaocalculo, somaetapas 
                FROM mig_acumulado_siga);", conn);
                acumulado.ExecuteNonQuery();

                MySqlCommand updatesInserts = new MySqlCommand($@"
                
                UPDATE acumulado a
                JOIN matricula m ON (a.MATRICULA = m.codmatricula)
                SET a.CODSERIE = m.codseriecurso
                WHERE a.CODSERIE != m.codseriecurso;

                
                UPDATE acumulado a
                JOIN matricula m ON (a.MATRICULA = m.codmatricula)
                SET a.CODTURMA = m.codturma
                WHERE a.CODTURMA != m.codturma;

                
                SET FOREIGN_KEY_CHECKS = 0; insert into professorturma (  CODSERIE  ,CODTURMA  ,ANO  ,CODMATERIA  ,CODMATERIAGRADE,reprova  ,codconfturma  ,reprovafalta  ,percentualnota )
                (select a.CODSERIE, a.CODTURMA, a.ANO, a.CODMATERIA, a.CODMATERIA as codmateriagrade, 'S', m.codconfturma, 'S', 1
                from acumulado a
                join matricula m on (a.MATRICULA = m.codmatricula)
                group by m.codconfturma, a.codmateria);", conn);

                updatesInserts.ExecuteNonQuery();

                ResetaValor.Resetar(out anoMenor, out anoMaior, FbConn);

                while (anoMenor <= anoMaior)
                {
                    MySqlCommand updatesInserts2 = new MySqlCommand($@"
                update acumulado a 
                join professorturma p on (a.CODMATERIA = p.CODMATERIA and a.ANO = p.ANO and a.CODSERIE = p.CODSERIE and a.CODTURMA = p.CODTURMA)
                set a.codprofturma = p.CODPROFTURMA
                where a.codprofturma is null
                and a.ano = {anoMenor};", conn);

                    updatesInserts2.ExecuteNonQuery();
                    anoMenor++;
                }

                MySqlCommand updatesInserts3 = new MySqlCommand($@"SET FOREIGN_KEY_CHECKS = 0; 
                -- ACUMULADOAGRUPADO 
                insert into acumuladoagrupado (matricula, codmateriagrade, anoletivo, codseriecurso, codturma, media1, soma1, rec1, faltas1, media2, soma2, rec2, faltas2, media3, soma3, rec3, faltas3, media4, soma4, rec4, faltas4,medsem1,medsem2,somasemestre1,somasemestre2,recsemestre1,recsemestre2,recfinal, somaetapas, resultadofinal, codaluno, totfaltas ,conceito1,conceito2,conceito3,conceito4,conceitofinal,aprovadoconselho,pontosaprovconselho )
                (select a.MATRICULA, a.CODMATERIA, a.ANO, a.CODSERIE, a.CODTURMA, a.MEDIA1, a.soma1, a.rec1, a.FALTAS1, a.MEDIA2, a.soma2, a.rec2, a.FALTAS2, a.MEDIA3, a.soma3, a.rec3, a.FALTAS3, a.MEDIA4, a.soma4, a.rec4, a.FALTAS4,a.MEDSEM1,a.MEDSEM2,a.somasemestre1, a.somasemestre2,a.RECSEMESTRE1,a.RECSEMESTRE2,a.recfinal, a.somaetapas, a.RESULTFINAL,m.codaluno,a.totfaltas, 
                aa.mig_conceito1 AS 'conceito1', aa.mig_conceito2 AS 'conceito2', aa.mig_conceito3 AS 'conceito3', aa.mig_conceito4 AS 'conceito4', aa.mig_conceitofinal AS 'conceitofinal', if(aa.mig_pontosaprovconselho IS NULL, NULL, 'S') AS 'aprovadoconselho' ,aa.mig_pontosaprovconselho AS 'pontosaprovconselho'
                from acumulado a 
                JOIN mig_acumulado_siga aa ON (a.CODACUMULADO = aa.CODACUMULADO)
                inner join professorturma p on (a.codprofturma = p.CODPROFTURMA)
                inner join matricula m on (a.MATRICULA = m.codmatricula));

                -- TURMA DIARIO 
                 insert into turmadiario (codmatricula, codaluno, anoletivo, codseriecurso, codturma, codconfturma, dtsaida)
                (select m.codmatricula, m.codaluno, m.anoletivo, m.codseriecurso, m.codturma, m.codconfturma, m.dtTransferencia
                from matricula m );", conn);
                updatesInserts3.ExecuteNonQuery();

                MessageBox.Show("Importação concluída com sucesso!");

            }
            catch (Exception err)
            {
                MessageBox.Show(err.Message);
            }
            finally
            {
                log.CommandText = "select count(1) from acumulado;";
                var qtd = Convert.ToInt32(log.ExecuteScalar());

                r.Record(args[8].ToString(), qtd);

                conn2.Close();
                conn.Close();
            }

        }
    }
}
