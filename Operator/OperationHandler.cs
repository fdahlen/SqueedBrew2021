using System.Data.SqlClient;
using System.Text;
using k8s;
using k8s.Models;
using ContainerSolutions.OperatorSDK;
using NLog;
using Microsoft.Rest;

namespace SqueedBrew2021Operator;

public class OperationHandler : IOperationHandler<Crd>
{
    private readonly Dictionary<string, Crd> currentState = new();

    private static readonly Logger Log = LogManager.GetCurrentClassLogger();

    public Task OnAdded(Kubernetes k8s, Crd crd)
    {
        lock (currentState)
        {
            CreateDB(k8s, crd);
        }

        return Task.CompletedTask;
    }

    public Task OnDeleted(Kubernetes k8s, Crd crd)
    {
        lock (currentState)
        {
            DeleteDB(k8s, crd);
        }

        return Task.CompletedTask;
    }

    public Task OnUpdated(Kubernetes k8s, Crd crd)
    {
        lock (currentState)
        {
            UpdateDB(k8s, crd);
        }

        return Task.CompletedTask;
    }

    public Task CheckCurrentState(Kubernetes k8s)
    {
        lock (currentState)
        {
            foreach (var key in currentState.Keys.ToList())
            {
                var crd = currentState[key];
                CreateDbIfNotExists(k8s, crd);
            }
        }

        return Task.CompletedTask;
    }

    public Task OnError(Kubernetes k8s, Crd crd)
    {
        Log.Error($"ERROR on {crd.Name()}");

        return Task.CompletedTask;
    }


    public Task OnBookmarked(Kubernetes k8s, Crd crd)
    {
        return Task.CompletedTask;
    }

    #region implementations

    private const string INSTANCE = "instance";
    private const string USER_ID = "userid";
    private const string PASSWORD = "password";
    private const string MASTER = "master";
    private void CreateDB(Kubernetes k8s, Crd db)
    {
        Log.Info($"Database {db.Spec.DBName} must be created.");

        using (SqlConnection connection = GetDBConnection(k8s, db))
        {
            connection.Open();

            try
            {
                SqlCommand createCommand = new SqlCommand($"CREATE DATABASE {db.Spec.DBName};", connection);
                int i = createCommand.ExecuteNonQuery();
            }
            catch (SqlException sex) when (sex.Number == 1801) //Database already exists
            {
                Log.Warn(sex.Message);
                currentState[db.Name()] = db;
                return;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex.Message);
                throw;
            }

            currentState[db.Name()] = db;
            Log.Info($"Database {db.Spec.DBName} successfully created!");
        }
    }

    private void DeleteDB(Kubernetes k8s, Crd crd)
    {
        Log.Info($"MSSQLDB {crd.Name()} must be deleted! ({crd.Spec.DBName})");

        using (SqlConnection connection = GetDBConnection(k8s, crd))
        {
            connection.Open();

            try
            {
                SqlCommand createCommand = new SqlCommand($"DROP DATABASE {crd.Spec.DBName};", connection);
                int i = createCommand.ExecuteNonQuery();
            }
            catch (SqlException sex)
            {
                if (sex.Number == 3701) //Already gone!
                {
                    Log.Error(sex.Message);
                    return;
                }

                Log.Error(sex.Message);
                return;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex.Message);
                return;
            }

            currentState.Remove(crd.Name());
            Log.Info($"Database {crd.Spec.DBName} successfully dropped!");
        }
    }

    private void UpdateDB(Kubernetes k8s, Crd crd)
    {
        Log.Info($"MSSQLDB {crd.Name()} was updated. ({crd.Spec.DBName})");

        Crd currentDb = currentState[crd.Name()];

        if (currentDb.Spec.DBName != crd.Spec.DBName)
        {
            try
            {
                RenameDB(k8s, currentDb, crd);
                Log.Info($"Database sucessfully renamed from {currentDb.Spec.DBName} to {crd.Spec.DBName}");
                currentState[crd.Name()] = crd;
            }
            catch (Exception ex)
            {
                Log.Fatal(ex);
                throw;
            }
        }
        else
        {
            currentState[crd.Name()] = crd;
        }
    }

    private void RenameDB(Kubernetes k8s, Crd currentDB, Crd newDB)
    {
        string sqlCommand = @$"ALTER DATABASE {currentDB.Spec.DBName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
ALTER DATABASE {currentDB.Spec.DBName} MODIFY NAME = {newDB.Spec.DBName};
ALTER DATABASE {newDB.Spec.DBName} SET MULTI_USER;";

        using (SqlConnection connection = GetDBConnection(k8s, newDB))
        {
            connection.Open();
            SqlCommand command = new SqlCommand(sqlCommand, connection);
            command.ExecuteNonQuery();
        }
    }

    private void CreateDbIfNotExists(Kubernetes k8s, Crd crd)
    {
        using var connection = GetDBConnection(k8s, crd);
        connection.Open();
        var queryCommand = new SqlCommand($"SELECT COUNT(*) FROM SYS.DATABASES WHERE NAME = '{crd.Spec.DBName}';", connection);

        try
        {
            var i = (int)queryCommand.ExecuteScalar();

            if (i == 0)
            {
                Log.Warn($"Database {crd.Spec.DBName} ({crd.Name()}) was not found!");
                CreateDB(k8s, crd);
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex.Message);
        }
    }

    private static SqlConnection GetDBConnection(Kubernetes k8s, Crd db)
    {
        var configMap = GetConfigMap(k8s, db);
        if (!configMap.Data.ContainsKey(INSTANCE))
            throw new ApplicationException($"ConfigMap '{configMap.Name()}' does not contain the '{INSTANCE}' data property.");

        string instance = configMap.Data[INSTANCE];

        var secret = GetSecret(k8s, db);
        if (!secret.Data.ContainsKey(USER_ID))
            throw new ApplicationException($"Secret '{secret.Name()}' does not contain the '{USER_ID}' data property.");

        if (!secret.Data.ContainsKey(PASSWORD))
            throw new ApplicationException($"Secret '{secret.Name()}' does not contain the '{PASSWORD}' data property.");

        string dbUser = ASCIIEncoding.UTF8.GetString(secret.Data[USER_ID]);
        string password = ASCIIEncoding.UTF8.GetString(secret.Data[PASSWORD]);

        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder
        {
            DataSource = instance,
            UserID = dbUser,
            Password = password,
            InitialCatalog = MASTER
        };

        return new SqlConnection(builder.ConnectionString);
    }

    private static V1ConfigMap GetConfigMap(Kubernetes k8s, Crd db)
    {
        try
        {
            return k8s.ReadNamespacedConfigMap(db.Spec.ConfigMap, db.Namespace());
        }
        catch (HttpOperationException hoex) when (hoex.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new ApplicationException($"ConfigMap '{db.Spec.ConfigMap}' not found in namespace {db.Namespace()}");
        }
    }

    private static V1Secret GetSecret(Kubernetes k8s, Crd db)
    {
        try
        {
            return k8s.ReadNamespacedSecret(db.Spec.Credentials, db.Namespace());
        }
        catch (HttpOperationException hoex) when (hoex.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new ApplicationException($"Secret '{db.Spec.Credentials}' not found in namespace {db.Namespace()}");
        }
    }

    #endregion
}