using System;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Configurations;
using ENode.Infrastructure;
using MySql.Data.MySqlClient;

namespace ENode.MySqlExtensions
{
    public class MySqlCommandStore : ICommandStore
    {
        #region Private Variables

        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly string _uniqueIndexName;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ITypeNameProvider _typeNameProvider;
        private readonly IOHelper _ioHelper;
        private readonly ILogger _logger;

        #region Constructors

        /// <summary>Default constructor.
        /// </summary>
        public MySqlCommandStore(OptionSetting optionSetting)
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
                _tableName = setting.CommandTableName;
                _uniqueIndexName = setting.CommandTableCommandIdUniqueIndexName;
            }

            Ensure.NotNull(_connectionString, "_connectionString");
            Ensure.NotNull(_tableName, "_tableName");
            Ensure.NotNull(_uniqueIndexName, "_uniqueIndexName");

            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        #endregion

        #endregion

        #region Public Methods

        public Task<AsyncTaskResult<CommandAddResult>> AddAsync(HandledCommand handledCommand)
        {
            var record = ConvertTo(handledCommand);

            return _ioHelper.TryIOFuncAsync(async () =>
            {
                try
                {
                    using (var connection = GetConnection())
                    {
                        string sql = string.Format(
                            "INSERT INTO `{0}` (CommandId,CreatedOn,AggregateRootId,MessagePayload,MessageTypeName) VALUES (@CommandId,@CreatedOn,@AggregateRootId,@MessagePayload,@MessageTypeName)",
                            _tableName);
                        await connection.ExecuteAsync(sql, record);
                        return new AsyncTaskResult<CommandAddResult>(AsyncTaskStatus.Success, null,
                            CommandAddResult.Success);
                    }
                }
                catch (MySqlException ex)
                {
                    if (ex.Number == 1062 && ex.Message.Contains(_uniqueIndexName))
                    {
                        return new AsyncTaskResult<CommandAddResult>(AsyncTaskStatus.Success, null, CommandAddResult.DuplicateCommand);
                    }
                    _logger.Error(ex.InnerException);
                    _logger.Error(string.Format("Add handled command has sql exception, handledCommand: {0}", handledCommand), ex);
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Add handled command has unkown exception, handledCommand: {0}", handledCommand), ex);
                    throw;
                }
            }, "AddCommandAsync");
        }
        public Task<AsyncTaskResult<HandledCommand>> GetAsync(string commandId)
        {
            return _ioHelper.TryIOFuncAsync(async () =>
            {
                try
                {
                    using (var connection = GetConnection())
                    {
                        string sql = string.Format("SELECT * FROM {0} WHERE CommandId=@CommandId", _tableName);
                        var result = await connection.QueryAsync<CommandRecord>(sql, new {CommandId = commandId});
                        var record = result.SingleOrDefault();
                        var handledCommand = record != null ? ConvertFrom(record) : null;
                        return new AsyncTaskResult<HandledCommand>(AsyncTaskStatus.Success, handledCommand);
                    }
                }
                catch (MySqlException ex)
                {
                    _logger.Error(string.Format("Get handled command has sql exception, commandId: {0}", commandId), ex);
                    return new AsyncTaskResult<HandledCommand>(AsyncTaskStatus.IOException, ex.Message, null);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Get handled command has unkown exception, commandId: {0}", commandId), ex);
                    return new AsyncTaskResult<HandledCommand>(AsyncTaskStatus.Failed, ex.Message, null);
                }
            }, "GetCommandAsync");
        }

        #endregion

        #region Private Methods

        private MySqlConnection GetConnection()
        {
            return new MySqlConnection(_connectionString);
        }
        private CommandRecord ConvertTo(HandledCommand handledCommand)
        {
            return new CommandRecord
            {
                CommandId = handledCommand.CommandId,
                AggregateRootId = handledCommand.AggregateRootId,
                MessagePayload = handledCommand.Message != null ? _jsonSerializer.Serialize(handledCommand.Message) : null,
                MessageTypeName = handledCommand.Message != null ? _typeNameProvider.GetTypeName(handledCommand.Message.GetType()) : null,
                CreatedOn = DateTime.Now,
            };
        }
        private HandledCommand ConvertFrom(CommandRecord record)
        {
            var message = default(IApplicationMessage);

            if (!string.IsNullOrEmpty(record.MessageTypeName))
            {
                var messageType = _typeNameProvider.GetType(record.MessageTypeName);
                message = _jsonSerializer.Deserialize(record.MessagePayload, messageType) as IApplicationMessage;
            }

            return new HandledCommand(record.CommandId, record.AggregateRootId, message);
        }

        #endregion

        public class CommandRecord
        {
            public string CommandId { get; set; }
            public string AggregateRootId { get; set; }
            public string MessagePayload { get; set; }
            public string MessageTypeName { get; set; }
            public DateTime CreatedOn { get; set; }
        }
    }
}
