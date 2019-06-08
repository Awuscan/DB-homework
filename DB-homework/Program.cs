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
        private static string DBAdress { set; get; } = ":memory:";

        //Adres Redis'a
        private static string CacheAdress { set; get; } = "localhost";

        //Czas przedawnienia danych
        private static TimeSpan TTL { set; get; } = new TimeSpan(0, 5, 0);
 
        //Połączenie z bazą danych
        public static IDbConnection DB { set; get; }

        //Połączenie z Redis'em
        public static IDatabase Cache { set; get; }

        static void Main()
        {
            //Utworzenie połączeń
            Init();

            //Zapisanie przykładowych danych
            SampleData();

            while (true)
            {
                Console.WriteLine("\nPodaj zapytanie: ");

                //Pobranie zapytania od użytkownika
                var input = Console.ReadLine();

                if (input == "") {}
                else
                if (input == "exit") { break; }
                else
                {
                    //Wykonanie zapytania
                    var result = GetQuery(input);

                    //Wyświetlenie wyniku
                    Console.WriteLine(result+ "\n");
                }
            }
        }

        //Pobranie danych z Redis'a
        static string ReadCache(String key) => Cache.StringGet(key);
 
        //Zapisanie danych do Redis'a
        static void WriteCache(String key, String value) => Cache.StringSet(key, value, TTL);

        //Wykonanie zapytania na bazie danych
        static string ReadDB(String SQLQuery)
        {
            List<Person> answer = null;
            try {
                answer = DB.Select<Person>(SQLQuery);
            }
            catch
            {
                return "Błąd zapytania";
            }

            return JsonConvert.SerializeObject(answer, Formatting.Indented);
        }

        //Pobranie wyniku zapytania użytkownika
        static string GetQuery(String SQLQuery)
        {
            var cacheKey = SQLQuery.ToUpper().Replace(" ", string.Empty);
            var cacheAnswer = ReadCache(cacheKey);
            if(cacheAnswer == null)
            {
                Console.WriteLine("\nOdczyt z bazy");
                var databaseAnswer = ReadDB(SQLQuery);
                if (databaseAnswer!= "Błąd zapytania")
                    WriteCache(cacheKey, databaseAnswer);
                return databaseAnswer;
            }
            else
            {
                Console.WriteLine("\nOdczyt z cache");
                return cacheAnswer;
            }
        }

        //Otwarcie połączeń z bazą danych i Redis'em
        static void Init()
        {
            try
            {
                var dbFactory = new OrmLiteConnectionFactory(DBAdress, SqliteDialect.Provider);
                DB = dbFactory.Open();
            }
            catch
            {
                Console.WriteLine("Błąd połączenia z bazą danych");
            }

            try
            {
                ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(CacheAdress);
                Cache = redis.GetDatabase();
            }
            catch
            {
                Console.WriteLine("Błąd połączenia z Redis'em");
            }
        }

        //Stworzenie tabeli i wypełnienie jej danymi
        static void SampleData()
        {
            try
            {
                if (DB.CreateTableIfNotExists<Person>())
                {  
                    StreamReader r = new StreamReader("file.json");
                    string json = r.ReadToEnd();
                    List<Person> persons = JsonConvert.DeserializeObject<List<Person>>(json);
                    DB.SaveAll(persons);
                }
            }
            catch
            {
                Console.WriteLine("Błąd odczytu pliku/zapisu do bazy danych");
            }
        }
    }
}