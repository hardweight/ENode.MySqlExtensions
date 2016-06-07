using ECommon.Configurations;
using ENode.Commanding;
using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.MySqlExtensions
{
    public static class ENodeConfigurationExtensions
    {
        private static readonly Configuration Configuration = Configuration.Instance;

        /// <summary>Use the MySqlLockService as the ILockService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlLockService(this ENodeConfiguration configuration,
            OptionSetting optionSetting = null)
        {
            Configuration.SetDefault<ILockService, MySqlLockService>(new MySqlLockService(optionSetting));
            return configuration;
        }

        /// <summary>Use the MySqlCommandStore as the ICommandStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlCommandStore(this ENodeConfiguration configuration,
            OptionSetting optionSetting = null)
        {
            Configuration.SetDefault<ICommandStore, MySqlCommandStore>(new MySqlCommandStore(optionSetting));
            return configuration;
        }


        /// <summary>Use the MySqlEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlEventStore(this ENodeConfiguration configuration, OptionSetting optionSetting = null)
        {
            Configuration.SetDefault<IEventStore, MySqlEventStore>(new MySqlEventStore(optionSetting));
            return configuration;
        }

        /// <summary>Use the SqlServerPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlPublishedVersionStore(this ENodeConfiguration configuration,
            OptionSetting optionSetting = null)
        {
            Configuration.SetDefault<IPublishedVersionStore, MySqlPublishedVersionStore>(
                new MySqlPublishedVersionStore(optionSetting));
            return configuration;
        }
    }
}
