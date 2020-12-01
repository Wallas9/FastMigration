using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace FastMigration
{
    static class ImportLookup
    {
        static public IImportArgs SearchForTableArgs(string nomeTabela)
        {
            var classeEncontrada = (from c in Assembly.GetExecutingAssembly().GetTypes()
                                    where c.GetCustomAttribute<ImportForAttribute>() != null &&
                                    c.GetCustomAttribute<ImportForAttribute>().NomeTabela.ToUpper().Equals(nomeTabela.ToUpper()) &&
                                    c.GetInterface(nameof(IImportArgs)) != null
                                    select c).Single();


            return Activator.CreateInstance(classeEncontrada) as IImportArgs;
        }

    }
}
