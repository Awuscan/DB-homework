using ServiceStack.OrmLite;
using StackExchange.Redis;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Data;
using System.Collections.Generic;
using DB_homework.Model;

namespace DB_homework
{
    class Program
    {
        //Adres bazy danych
        private static string dbAdress { set; get; } = ":memory:";

        //Adres Redis'a
        private static string cacheAdress { set; get; } = "localhost";

        //Czas przedawnienia danych
        private static TimeSpan ttl { set; get; } = new TimeSpan(0, 5, 0);
 
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

                if (input == "") {}
                else
                if (input == "exit") { break; }
                else
                {
                    //Odczyt zapytania
                    var result = getQuery(input);

                    //Wyświetlenie wyniku
                    Console.WriteLine(result);
                }

            }
        }

        static string readCache(String key) => cache.StringGet(key.ToUpper());
 
        static void writeCache(String key, String value) => cache.StringSet(key.ToUpper().Replace(" ", string.Empty), value, ttl);

        static string readDB(String SQLQuery)
        {
            List<Person> answer = null;
            try {
                answer = db.Select<Person>(SQLQuery);
            }
            catch
            {
                return "ZŁA SKŁADNIA";
            }

            return JsonConvert.SerializeObject(answer, Formatting.Indented);
        }
        static string getQuery(String SQLQuery)
        {
            var cacheKey = SQLQuery;
            var cacheAnswer = readCache(cacheKey);
            if(cacheAnswer == null)
            {
                Console.WriteLine("odczyt z bazy");
                var databaseAnswer = readDB(SQLQuery);
                if (databaseAnswer!= "ZŁA SKŁADNIA")
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
            try
            {
                var dbFactory = new OrmLiteConnectionFactory(dbAdress, SqliteDialect.Provider);
                db = dbFactory.Open();
            }
            catch
            {
                Console.WriteLine("Błąd połączenia z bazą danych");
            }

            try
            {
                var redis = ConnectionMultiplexer.Connect(cacheAdress);
                cache = redis.GetDatabase();
            }
            catch
            {
                Console.WriteLine("Błąd połączenia z Redis'em");
            }

        }

        static void SampleData()
        {
            try
            {
                if (db.CreateTableIfNotExists<Person>())
                {
             
                    StreamReader r = new StreamReader("file.json");
                    string json = r.ReadToEnd();
                    List<Person> persons = JsonConvert.DeserializeObject<List<Person>>(json);
                    db.SaveAll(persons);
                }
            }
            catch
            {
                Console.WriteLine("Błąd odczytu pliku/zapisu do bazy danych");
            }
        }
    }
}
