using System;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Utilities;
using ENode.Configurations;
using ENode.Infrastructure;
using MySql.Data.MySqlClient;

namespace ENode.MySqlExtensions
{
    public class MySqlPublishedVersionStore : IPublishedVersionStore
    {
        #region Private Variables

        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly string _uniqueIndexName;
        private readonly ILogger _logger;

        #endregion

        #region Constructors

        public MySqlPublishedVersionStore(OptionSetting optionSetting)
        {
            if (optionSetting != null)
            {
                _connectionString = optionSetting.GetOptionValue<string>("ConnectionString");
                _tableName = optionSetting.GetOptionValue<string>("TableName");
                _uniqueIndexName = optionSetting.GetOptionValue<string>("UniqueIndexName");
            }
            else
            {
                var setting = ENodeConfiguration.Instance.Setting.DefaultDBConfigurationSetting;
                _connectionString = setting.ConnectionString;
                _tableName = setting.PublishedVersionTableName;
                _uniqueIndexName = setting.PublishedVersionUniqueIndexName;
            }

            Ensure.NotNull(_connectionString, "_connectionString");
            Ensure.NotNull(_tableName, "_tableName");
            Ensure.NotNull(_uniqueIndexName, "_uniqueIndexName");

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        #endregion

        public async Task<AsyncTaskResult> UpdatePublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId, int publishedVersion)
        {
            if (publishedVersion == 1)
            {
                try
                {
                    using (var connection = GetConnection())
                    {
                        string sql = string.Format(
                            "INSERT INTO {0} (ProcessorName,AggregateRootTypeName,AggregateRootId,Version,CreatedOn) VALUES (@ProcessorName,@AggregateRootTypeName,@AggregateRootId,@Version,@CreatedOn)",
                            _tableName);

                        await connection.ExecuteAsync(sql, new
                        {
                            ProcessorName = processorName,
                            AggregateRootTypeName = aggregateRootTypeName,
                            AggregateRootId = aggregateRootId,
                            Version = 1,
                            CreatedOn = DateTime.Now
                        });
                        return AsyncTaskResult.Success;
                    }
                }
                catch (MySqlException ex)
                {
                    if (ex.Number == 1062 && ex.Message.Contains(_uniqueIndexName))
                    {
                        return AsyncTaskResult.Success;
                    }
                    _logger.Error("Insert aggregate published version has sql exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.IOException, ex.Message);
                }
                catch (Exception ex)
                {
                    _logger.Error("Insert aggregate published version has unknown exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.Failed, ex.Message);
                }
            }
            else
            {
                try
                {
                    using (var connection = GetConnection())
                    {
                        string sql = string.Format(
                            "UPDATE {0} SET Version=@Version,CreatedOn=@CreatedOn WHERE ProcessorName=@ProcessorName AND AggregateRootId=@AggregateRootId AND Version=@NVersion",
                            _tableName);
                        await connection.ExecuteAsync(sql, new
                        {
                            Version = publishedVersion,
                            ProcessorName = processorName,
                            AggregateRootId = aggregateRootId,
                            NVersion = publishedVersion - 1
                        });
                        return AsyncTaskResult.Success;
                    }
                }
                catch (MySqlException ex)
                {
                    _logger.Error("Update aggregate published version has sql exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.IOException, ex.Message);
                }
                catch (Exception ex)
                {
                    _logger.Error("Update aggregate published version has unknown exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.Failed, ex.Message);
                }
            }
        }
        public async Task<AsyncTaskResult<int>> GetPublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId)
        {
            try
            {
                using (var connection = GetConnection())
                {
                    string sql = string.Format("SELECT Version FROM {0} WHERE ProcessorName=@ProcessorName AND AggregateRootId=@AggregateRootId", _tableName);
                    var result = await connection.QueryAsync<int>(sql, new
                    {
                        ProcessorName = processorName,
                        AggregateRootId = aggregateRootId
                    });
                    return new AsyncTaskResult<int>(AsyncTaskStatus.Success, result.SingleOrDefault());
                }
            }
            catch (MySqlException ex)
            {
                _logger.Error("Get aggregate published version has sql exception.", ex);
                return new AsyncTaskResult<int>(AsyncTaskStatus.IOException, ex.Message);
            }
            catch (Exception ex)
            {
                _logger.Error("Get aggregate published version has unknown exception.", ex);
                return new AsyncTaskResult<int>(AsyncTaskStatus.Failed, ex.Message);
            }
        }

        private MySqlConnection GetConnection()
        {
            return new MySqlConnection(_connectionString);
        }
    }
}
