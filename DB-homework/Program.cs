using ServiceStack.OrmLite;
using StackExchange.Redis;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Collections.Generic;
using DB_homework.Model;


namespace DB_homework
{
    class Program
    {
        private static string dbAdress { set; get; } = ":memory:";
        private static string cacheAdress { set; get; } = "localhost";
        private static TimeSpan ttl { set; get; } = new TimeSpan(0, 0, 5, 0);
 
        public static IDbConnection db;
        public static IDatabase cache;
        static void Main(string[] args)
        {
            Init();
            SampleData();

            //Przykładowe zapytanie
            SqlExpression<Person> SQLQuery = db.From<Person>().Join<Address>().Where(x => x.Age>17).Select();
            SqlExpression<Address> SQLQuery2 = db.From<Address>().Where().Select();

            var result = getQuery(SQLQuery2);
            Console.WriteLine(result);

            //List<Person> resultTwo = db.Select<Person>("SELECT Id, Name FROM Persons WHERE Age > 10"); // Przez SQL
           // List<Person> result = db.Select(SQLQuery);
            //Console.WriteLine(result);


            //if(cacheResult)
            //{
            //    List<Person> result = db.Select(SQLQuery);
            //    result.GetType();
            //    cache.StringSet(SQLQuery.ToSelectStatement().ToString(), JsonConvert.SerializeObject(result), ttl);
            //    Console.WriteLine("odczyt z bazy");
            //    Console.WriteLine(JsonConvert.SerializeObject(result));
            //}
            //else
            //{
            //    var result = JsonConvert.DeserializeObject(cacheResult);
            //    Console.WriteLine("odczyt z chache");
            //    Console.WriteLine(result);
            //}
        }

        static string readCache(ISqlExpression SQLQuery) => cache.StringGet(SQLQuery.ToSelectStatement().ToString());
 
        static void writeCache(String key, String value) => cache.StringSet(key, value, ttl);

        static string readDB(SqlExpression<Person> SQLQuery) => JsonConvert.SerializeObject(db.Select(SQLQuery));
        static string readDB(SqlExpression<Address> SQLQuery) => JsonConvert.SerializeObject(db.Select(SQLQuery));

        static string getQuery(SqlExpression<Person> SQLQuery)
        {
            var cacheAnswer = readCache(SQLQuery);
            if(cacheAnswer == null)
            {
                Console.WriteLine("odczyt z bazy");
                var databaseAnswer = readDB(SQLQuery);
                databaseAnswer = JsonConvert.SerializeObject(databaseAnswer);
                writeCache(SQLQuery.ToSelectStatement().ToString(), databaseAnswer);
                return (string)JsonConvert.DeserializeObject(databaseAnswer);
            }
            else
            {
                Console.WriteLine("odczyt z cache");
                return (string)JsonConvert.DeserializeObject(cacheAnswer);
            }
        }
        static string getQuery(SqlExpression<Address> SQLQuery)
        {
            var cacheAnswer = readCache(SQLQuery);
            if (cacheAnswer == null)
            {
                Console.WriteLine("odczyt z bazy");
                var databaseAnswer = readDB(SQLQuery);
                databaseAnswer = JsonConvert.SerializeObject(databaseAnswer);
                writeCache(SQLQuery.ToSelectStatement().ToString(), databaseAnswer);
                return (string)JsonConvert.DeserializeObject(databaseAnswer);
            }
            else
            {
                Console.WriteLine("odczyt z cache");
                return (string)JsonConvert.DeserializeObject(cacheAnswer);
            }
        }

        static void Init()
        {
            var dbFactory = new OrmLiteConnectionFactory(dbAdress, SqliteDialect.Provider);
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(cacheAdress);
            cache = redis.GetDatabase();
            db = dbFactory.Open();
            db.CreateTableIfNotExists<Address>();
            db.CreateTableIfNotExists<Person>();

        }
        static void SampleData()
        {
            Address addressOne = new Address() { Text = "Słoneczny Rzeszów" };
            Address addressTwo = new Address() { Text = "Boguchwała" };

            Person personOne = new Person() { Name = "Maciej", Surname = "Penar", Age = 17, Address = addressOne };
            Person personTwo = new Person() { Name = "Wanda", Surname = "Kowalska", Age = 20, Address = addressOne };
            Person personThree = new Person() { Name = "Róża", Surname = "Wesoła", Age = 21, Address = addressTwo };

            db.SaveAllReferences(personOne);
            db.Save(personOne);

            db.SaveAllReferences(personTwo);
            db.Save(personTwo);

            db.SaveAllReferences(personThree);
            db.Save(personThree);
        }
    }
}
