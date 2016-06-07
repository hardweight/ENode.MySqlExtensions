using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using ECommon.Utilities;
using ENode.MySqlExtensions;
using MySql.Data.MySqlClient;
using Xunit;

namespace ENode.MySqlExtensionsUnitTest
{
    public class MySqlCommandStoreTest
    {
        [Fact]
        public void MySqlCommandStore_Insert_Test()
        {
            using (
                var connection = new MySqlConnection(ConfigurationManager.ConnectionStrings["ENode"].ConnectionString))
            {
                MySqlCommandStore.CommandRecord record = new MySqlCommandStore.CommandRecord()
                {
                    AggregateRootId = ObjectId.GenerateNewStringId(),
                    CommandId = ObjectId.GenerateNewStringId(),
                    CreatedOn = DateTime.Now,
                    MessagePayload =null,
                    MessageTypeName = "qweqewqwe"
                };

                string sql = string.Format(
                    "INSERT INTO `{0}` (CommandId,CreatedOn,AggregateRootId,MessagePayload,MessageTypeName) VALUES (@CommandId,@CreatedOn,@AggregateRootId,@MessagePayload,@MessageTypeName)",
                    "Command");
                var r = connection.Execute(sql, record);
                Assert.Equal(1, r);
            }
        }
    }
}
