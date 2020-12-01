using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data.MySqlClient;
using FirebirdSql.Data.FirebirdClient;
using System.Windows.Forms;

namespace FastMigration.Metodos
{
    public class ResetaValorAcumulado
    {

        public static void Resetar(out int a, out int b, string fbconn)
        {

            FbConnection conn2 = new FbConnection(fbconn);
            conn2.Open();

            FbCommand anoAcumuladoInicio = new FbCommand(@"select first 1 a.anoletivo
                from signotfa a
                join sigaluno m on (a.unidade = m.unidade and a.anoletivo = m.anoletivo and a.periodo = m.periodo and a.curso = m.curso and a.serie = m.serie and a.classe = m.classe and a.turno = m.turno and a.chamada = m.chamada )
                join sigclass t on (a.unidade = t.unidade and a.anoletivo = t.anoletivo and a.periodo = t.periodo and a.curso = t.curso and a.serie = t.serie and a.classe = t.classe and a.turno = t.turno)
                join turma_tella tt on tt.dscturma = (COALESCE(t.classe,'X') || COALESCE(t.turno,'Y') || COALESCE(t.periodo,'Z'))
                join sigdisci d on (d.codigo = a.disciplina)
                where a.anoletivo > 1000
                order by a.anoletivo", conn2);

            FbCommand anoAcumuladoFim = new FbCommand(@"select first 1 a.anoletivo
                from signotfa a
                join sigaluno m on (a.unidade = m.unidade and a.anoletivo = m.anoletivo and a.periodo = m.periodo and a.curso = m.curso and a.serie = m.serie and a.classe = m.classe and a.turno = m.turno and a.chamada = m.chamada )
                join sigclass t on (a.unidade = t.unidade and a.anoletivo = t.anoletivo and a.periodo = t.periodo and a.curso = t.curso and a.serie = t.serie and a.classe = t.classe and a.turno = t.turno)
                join turma_tella tt on tt.dscturma = (COALESCE(t.classe,'X') || COALESCE(t.turno,'Y') || COALESCE(t.periodo,'Z'))
                join sigdisci d on (d.codigo = a.disciplina)
                where a.anoletivo > 1000
                order by a.anoletivo DESC", conn2);

            a = 0; b = 0;
            Int32.TryParse(anoAcumuladoInicio.ExecuteScalar().ToString(), out a);
            Int32.TryParse(anoAcumuladoFim.ExecuteScalar().ToString(), out b);

        }
    }
}
