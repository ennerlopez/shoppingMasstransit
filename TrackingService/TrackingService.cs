using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using Automatonymous;
using MassTransit.NHibernateIntegration;
using MassTransit.NHibernateIntegration.Saga;
using NHibernate;
using NHibernate.Cache;
using NHibernate.Cfg;
using NHibernate.Cfg.Loquacious;
using NHibernate.Dialect;
using NHibernate.Engine;
using NHibernate.Metadata;
using NHibernate.Stat;
using NHibernate.Tool.hbm2ddl;
using NHibernateSessionFactoryProvider = Automatonymous.NHibernateSessionFactoryProvider;

namespace TrackingService
{
    using System;
    using System.Configuration;
    using CartTracking;
    using MassTransit;
    using MassTransit.QuartzIntegration;
    using MassTransit.RabbitMqTransport;
    using MassTransit.Saga;
    using Quartz;
    using Quartz.Impl;
    using Topshelf;
    using Topshelf.Logging;


    class TrackingService :
        ServiceControl
    {
        readonly LogWriter _log = HostLogger.Get<TrackingService>();
        readonly IScheduler _scheduler;

        IBusControl _busControl;
        BusHandle _busHandle;
        ShoppingCartStateMachine _machine;
        Lazy<ISagaRepository<ShoppingCart>> _repository;

        public TrackingService()
        {
            _scheduler = CreateScheduler();
        }

        public bool Start(HostControl hostControl)
        {
            _log.Info("Creating bus...");

            _machine = new ShoppingCartStateMachine();

            //SagaDbContextFactory sagaDbContextFactory =
            //    () => new SagaDbContext<ShoppingCart, ShoppingCartMap>(SagaDbContextFactoryProvider.ConnectionString);

            _repository = new Lazy<ISagaRepository<ShoppingCart>>(
                () => new NHibernateSagaRepository<ShoppingCart>(CreateImageRetrievalSessionFactory()));

            _busControl = Bus.Factory.CreateUsingRabbitMq(x =>
            {
                IRabbitMqHost host = x.Host(new Uri(ConfigurationManager.AppSettings["RabbitMQHost"]), h =>
                {
                    h.Username("guest");
                    h.Password("guest");
                });

                x.ReceiveEndpoint(host, "shopping_cart_state", e =>
                {
                    e.PrefetchCount = 8;
                    e.StateMachineSaga(_machine, _repository.Value);
                });

                x.ReceiveEndpoint(host, ConfigurationManager.AppSettings["SchedulerQueueName"], e =>
                {
                    x.UseMessageScheduler(e.InputAddress);
                    e.PrefetchCount = 1;

                    e.Consumer(() => new ScheduleMessageConsumer(_scheduler));
                    e.Consumer(() => new CancelScheduledMessageConsumer(_scheduler));
                });
            });

            _log.Info("Starting bus...");

            try
            {
                _busHandle = _busControl.Start();

                _scheduler.JobFactory = new MassTransitJobFactory(_busControl);

                _scheduler.Start();
            }
            catch (Exception)
            {
                _scheduler.Shutdown();
                throw;
            }

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            _log.Info("Stopping bus...");

            _scheduler.Standby();

            if (_busHandle != null)
                _busHandle.Stop();

            _scheduler.Shutdown();

            return true;
        }


        static IScheduler CreateScheduler()
        {
            ISchedulerFactory schedulerFactory = new StdSchedulerFactory();

            IScheduler scheduler = schedulerFactory.GetScheduler();

            return scheduler;
        }




        ISessionFactory CreateImageRetrievalSessionFactory()
        {
            var connectionStringProvider = SagaDbContextFactoryProvider.ConnectionString;



            return new SQLiteSessionFactoryProvider(connectionStringProvider, typeof(ShoppingCartMap))
                .GetSessionFactory();


            return new SQLiteSessionFactoryProvider(typeof(ShoppingCartMap))
                .GetSessionFactory();
        }
    }

    public class SQLiteSessionFactoryProvider :
       NHibernateSessionFactoryProvider,
       IDisposable
    {
        const string InMemoryConnectionString = "Data Source=:memory:;Version=3;New=True;Pooling=True;Max Pool Size=1;";
        bool _disposed;
        ISessionFactory _innerSessionFactory;
        SQLiteConnection _openConnection;
        SingleConnectionSessionFactory _sessionFactory;

        public SQLiteSessionFactoryProvider(string connectionString, params Type[] mappedTypes)
            : base(mappedTypes, x => Integrate(x, connectionString, false))
        {
        }

        public SQLiteSessionFactoryProvider(params Type[] mappedTypes)
            : this(false, mappedTypes)
        {
        }

        public SQLiteSessionFactoryProvider(bool logToConsole, params Type[] mappedTypes)
            : base(mappedTypes, x => Integrate(x, null, logToConsole))
        {
            Configuration.SetProperty(NHibernate.Cfg.Environment.UseSecondLevelCache, "true");
            Configuration.SetProperty(NHibernate.Cfg.Environment.UseQueryCache, "true");
            Configuration.SetProperty(NHibernate.Cfg.Environment.CacheProvider,
                typeof(HashtableCacheProvider).AssemblyQualifiedName);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            if (disposing)
            {
                if (_openConnection != null)
                {
                    _openConnection.Close();
                    _openConnection.Dispose();
                }
            }

            _disposed = true;
        }

        public override ISessionFactory GetSessionFactory()
        {
            string connectionString = Configuration.Properties[NHibernate.Cfg.Environment.ConnectionString];
            _openConnection = new SQLiteConnection(connectionString);
            _openConnection.Open();

            //            BuildSchema(Configuration, _openConnection);

            _innerSessionFactory = base.GetSessionFactory();
            _innerSessionFactory.OpenSession(_openConnection);

            _sessionFactory = new SingleConnectionSessionFactory(_innerSessionFactory, _openConnection);

            return _sessionFactory;
        }

        static void BuildSchema(NHibernate.Cfg.Configuration config, IDbConnection connection)
        {
            new SchemaExport(config).Execute(true, true, false, connection, null);
        }

        static void Integrate(IDbIntegrationConfigurationProperties db, string connectionString, bool logToConsole)
        {
            db.Dialect<SQLiteDialect>();
            db.ConnectionString = connectionString ?? InMemoryConnectionString;
            db.BatchSize = 100;
            db.IsolationLevel = IsolationLevel.Serializable;
            db.LogSqlInConsole = logToConsole;
            db.LogFormattedSql = logToConsole;
            db.KeywordsAutoImport = Hbm2DDLKeyWords.AutoQuote;
            db.SchemaAction = SchemaAutoAction.Update;
        }
    }

    public class SingleConnectionSessionFactory :
      ISessionFactory
    {
        readonly ISessionFactory _inner;
        readonly IDbConnection _liveConnection;

        public SingleConnectionSessionFactory(ISessionFactory inner, IDbConnection liveConnection)
        {
            _inner = inner;
            _liveConnection = liveConnection;
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        public ISession OpenSession(IDbConnection conn)
        {
            return _inner.OpenSession(_liveConnection);
        }

        public ISession OpenSession(IInterceptor sessionLocalInterceptor)
        {
            return _inner.OpenSession(_liveConnection, sessionLocalInterceptor);
        }

        public ISession OpenSession(IDbConnection conn, IInterceptor sessionLocalInterceptor)
        {
            return _inner.OpenSession(_liveConnection, sessionLocalInterceptor);
        }

        public ISession OpenSession()
        {
            return _inner.OpenSession(_liveConnection);
        }

        public IClassMetadata GetClassMetadata(Type persistentClass)
        {
            return _inner.GetClassMetadata(persistentClass);
        }

        public IClassMetadata GetClassMetadata(string entityName)
        {
            return _inner.GetClassMetadata(entityName);
        }

        public ICollectionMetadata GetCollectionMetadata(string roleName)
        {
            return _inner.GetCollectionMetadata(roleName);
        }

        public IDictionary<string, IClassMetadata> GetAllClassMetadata()
        {
            return _inner.GetAllClassMetadata();
        }

        public IDictionary<string, ICollectionMetadata> GetAllCollectionMetadata()
        {
            return _inner.GetAllCollectionMetadata();
        }

        public void Close()
        {
            _inner.Close();
        }

        public void Evict(Type persistentClass)
        {
            _inner.Evict(persistentClass);
        }

        public void Evict(Type persistentClass, object id)
        {
            _inner.Evict(persistentClass, id);
        }

        public void EvictEntity(string entityName)
        {
            _inner.EvictEntity(entityName);
        }

        public void EvictEntity(string entityName, object id)
        {
            _inner.EvictEntity(entityName, id);
        }

        public void EvictCollection(string roleName)
        {
            _inner.EvictCollection(roleName);
        }

        public void EvictCollection(string roleName, object id)
        {
            _inner.EvictCollection(roleName, id);
        }

        public void EvictQueries()
        {
            _inner.EvictQueries();
        }

        public void EvictQueries(string cacheRegion)
        {
            _inner.EvictQueries(cacheRegion);
        }

        public IStatelessSession OpenStatelessSession()
        {
            return _inner.OpenStatelessSession();
        }

        public IStatelessSession OpenStatelessSession(IDbConnection connection)
        {
            return _inner.OpenStatelessSession(connection);
        }

        public FilterDefinition GetFilterDefinition(string filterName)
        {
            return _inner.GetFilterDefinition(filterName);
        }

        public ISession GetCurrentSession()
        {
            return _inner.GetCurrentSession();
        }

        public IStatistics Statistics
        {
            get { return _inner.Statistics; }
        }

        public bool IsClosed
        {
            get { return _inner.IsClosed; }
        }

        public ICollection<string> DefinedFilterNames
        {
            get { return _inner.DefinedFilterNames; }
        }
    }
}