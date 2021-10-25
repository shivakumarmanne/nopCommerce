using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Nop.Core;
using Nop.Core.Domain.Catalog;
using Nop.Core.Domain.Gdpr;
using Nop.Data;
using Nop.Web.Models.Install;
using NUnit.Framework;

namespace Nop.Tests.Nop.DataTests
{
    [TestFixture]
    public class NopDataProviderTests : BaseNopTest
    {
        [TearDown]
        public async Task TearDown()
        {
            try
            {
                var dataProvider = GetService<INopDataProvider>();
                await dataProvider.TruncateAsync<GdprConsent>(true);
            }
            catch
            {
                //ignore 
            }

            SetDataProviderType(DataProviderType.Unknown);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanCreateDatabase(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanInitializeDatabase(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanCreateForeignKeyName(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanGetIndexName(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanCreateTempDataStorage(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();
            var productRepository = GetService<IRepository<Product>>();

            var tableName = "TestTempDataTable".ToLower();

            await using var data = await dataProvider.CreateTempDataStorageAsync(tableName,
                productRepository.Table.Select(p => new { p.Name, p.Id, p.Deleted }));

            data.Count().Should().Be(productRepository.GetAll(query => query).Count);

            var rez = await dataProvider.QueryAsync<object>($"select * from {tableName}");

            rez.Count.Should().Be(data.Count());
        }
        
        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanInsertEntity(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            await dataProvider.InsertEntityAsync(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.TruncateAsync<GdprConsent>();
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanUpdateEntity(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            await dataProvider.UpdateEntityAsync(new GdprConsent
            {
                Id = gdprConsent.Id,
                Message = "Updated test tax category"
            });
            var updatedGdprConsent = dataProvider.GetTable<GdprConsent>().FirstOrDefault(tc=>tc.Id == gdprConsent.Id);
            await dataProvider.TruncateAsync<GdprConsent>();

            gdprConsent.Id.Should().BeGreaterThan(0);
            updatedGdprConsent?.Message.Should().NotBeEquivalentTo(gdprConsent.Message);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanUpdateEntities(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            await dataProvider.UpdateEntitiesAsync(new []
            {
                new GdprConsent
                {
                    Id = gdprConsent.Id,
                    Message = "Updated test tax category"
                }
            });
            var updatedGdprConsent = dataProvider.GetTable<GdprConsent>().FirstOrDefault(tc => tc.Id == gdprConsent.Id);
            await dataProvider.TruncateAsync<GdprConsent>();

            gdprConsent.Id.Should().BeGreaterThan(0);
            updatedGdprConsent?.Message.Should().NotBeEquivalentTo(gdprConsent.Message);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanDeleteEntity(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.DeleteEntityAsync(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanBulkDeleteEntities(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.BulkDeleteEntitiesAsync(new List<GdprConsent> { gdprConsent });
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.BulkDeleteEntitiesAsync<GdprConsent>( _ => true);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanBulkInsertEntities(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };

            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            await dataProvider.BulkInsertEntitiesAsync(new[] { gdprConsent });
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.TruncateAsync<GdprConsent>();
        }
        
        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanGetTable(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>();
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            dataProvider.InsertEntity(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(1);
            await dataProvider.DeleteEntityAsync(gdprConsent);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanGetTableIdentAsync(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var gdprConsent = new GdprConsent { DisplayOrder = 10, Message = "Test message 1" };
            await dataProvider.TruncateAsync<GdprConsent>(true);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            var nextId = await dataProvider.GetTableIdentAsync<GdprConsent>();

            nextId.Should().Be(type == DataProviderType.Unknown ? 0 : 1);

            dataProvider.InsertEntity(gdprConsent);

            nextId = await dataProvider.GetTableIdentAsync<GdprConsent>();
            if (type == DataProviderType.SqlServer || type == DataProviderType.Unknown)
                nextId.Should().Be(1);
            else
                nextId.Should().Be(2);

            dataProvider.InsertEntity(gdprConsent);

            nextId = await dataProvider.GetTableIdentAsync<GdprConsent>();

            if (type == DataProviderType.SqlServer || type == DataProviderType.Unknown)
                nextId.Should().Be(2);
            else
                nextId.Should().Be(3);

            await dataProvider.TruncateAsync<GdprConsent>(true);
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanBuildConnectionString(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            var connStr = dataProvider.BuildConnectionString(new InstallModel
            {
                DatabaseName = "test_db",
                ServerName = "127.0.0.1",
                IntegratedSecurity = false,
                Username = "test",
                Password = "passwd"
            });

            switch (type)
            {
                case DataProviderType.Unknown:
                    connStr.Should()
                        .Be(@"Data Source=D:\git\nopCommerce\src\Presentation\Nop.Web\App_Data\test_db.sqlite;Mode=ReadWrite;Cache=Shared;Password=passwd");
                    break;
                case DataProviderType.SqlServer:
                    connStr.Should()
                        .Be(@"Data Source=127.0.0.1;Initial Catalog=test_db;Integrated Security=False;Persist Security Info=False;User ID=test;Password=passwd;Trust Server Certificate=True");
                    break;
                case DataProviderType.MySql:
                    connStr.Should()
                        .Be(@"server=127.0.0.1;database=test_db;allowuservariables=True;user id=test;password=passwd");
                    break;
                case DataProviderType.PostgreSQL:
                    connStr.Should()
                        .Be(@"Host=127.0.0.1;Database=test_db;Username=test;Password=passwd");
                    break;
            }

            Assert.Throws<ArgumentNullException>(() => dataProvider.BuildConnectionString(null));

            if (type == DataProviderType.SqlServer)
                return;

            Assert.Throws<NopException>(() => dataProvider.BuildConnectionString(new InstallModel()
            {
                IntegratedSecurity = true
            }));
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanSetTableIdent(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;

            var dataProvider = GetService<INopDataProvider>();

            await dataProvider.TruncateAsync<GdprConsent>(true);
            dataProvider.GetTable<GdprConsent>().Count().Should().Be(0);
            var nextId = await dataProvider.GetTableIdentAsync<GdprConsent>();

            nextId.Should().Be(type == DataProviderType.Unknown ? 0 : 1);

            await dataProvider.SetTableIdentAsync<GdprConsent>(10);
            nextId = await dataProvider.GetTableIdentAsync<GdprConsent>();

            nextId.Should().Be(10);
        }
        
        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanExecuteNonQuery(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanQueryProc(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanQuery(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }

        [Test]
        [TestCase(DataProviderType.Unknown)]
        [TestCase(DataProviderType.SqlServer)]
        [TestCase(DataProviderType.MySql)]
        [TestCase(DataProviderType.PostgreSQL)]
        public async Task CanTruncate(DataProviderType type)
        {
            if (!SetDataProviderType(type))
                return;
        }
    }
}
