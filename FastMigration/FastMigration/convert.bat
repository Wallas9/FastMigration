echo off
echo *** INICIANDO ***
echo RESTAURANDO BACKUP
cd C:\Program Files\Firebird\Firebird_2_5\bin
gbak -r -user SYSDBA -password masterkey c:\FastMigration\backup.fbk localhost:c:\FastMigration\dados.fdb



