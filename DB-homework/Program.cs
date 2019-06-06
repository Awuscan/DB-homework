using ServiceStack.OrmLite;
using StackExchange.Redis;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Collections.Generic;
using DB_homework.Model;
using System.IO;

namespace DB_homework
{
    class Program
    {
        //Adres bazy danych
        private static string dbAdress { set; get; } = ":memory:";
        //Adres Redis'a
        private static string cacheAdress { set; get; } = "localhost";
        //Czas życia zapisu danych na serwerze chache
        private static TimeSpan ttl { set; get; } = new TimeSpan(0, 0, 5, 0);
 
        public static IDbConnection db;
        public static IDatabase cache;
        static void Main(string[] args)
        {
            //Utworzenie połączeń
            Init();

            //Zapisanie przykładowych danych
            SampleData();

            while (true)
            {
                var input = Console.ReadLine();

                if(input == "exit")
                {
                    break;
                }

                var Query = input;

                //Przykładowe zapytanie
                //String Query = "SELECT Name FROM Persons WHERE Age < 50";

                //Odczyt zapytania
                var result = getQuery(Query);

                //Wyświetlenie wyniku
                Console.WriteLine(result);
            }
        }

        static string readCache(String key) => cache.StringGet(key);
 
        static void writeCache(String key, String value) => cache.StringSet(key, value, ttl);

        static string readDB(String SQLQuery) => JsonConvert.SerializeObject(db.Select<Person>(SQLQuery), Formatting.Indented);
      
        static string getQuery(String SQLQuery)
        {
            var cacheKey = SQLQuery;
            var cacheAnswer = readCache(cacheKey);
            if(cacheAnswer == null)
            {
                Console.WriteLine("odczyt z bazy");
                var databaseAnswer = readDB(SQLQuery);
                writeCache(cacheKey, databaseAnswer);
                return databaseAnswer;
            }
            else
            {
                Console.WriteLine("odczyt z cache");
                return cacheAnswer;
            }
        }
        
        static void Init()
        {
            var dbFactory = new OrmLiteConnectionFactory(dbAdress, SqliteDialect.Provider);
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(cacheAdress);
            cache = redis.GetDatabase();
            db = dbFactory.Open();
        }

        static void SampleData()
        {
            if (db.CreateTableIfNotExists<Person>())
            {
                StreamReader r = new StreamReader("c:/users/mateusz/documents/github/db-homework/db-homework/file.json");
                string json = r.ReadToEnd();
                List<Person> persons = JsonConvert.DeserializeObject<List<Person>>(json);


                foreach (var person in persons)
                {
                    db.Save(person);
                }
            }
        }
    }
}
