using ServiceStack.DataAnnotations;
using System;

namespace DB_homework.Model
{
    [Alias("Persons")] 
    public class Person
    {
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        [Required]
        public string Surname { get; set; }

        [Default(-1)]
        [CheckConstraint("Age >= -1")]
        public int Age { get; set; }

        [Reference]
        public Address Address { get; set; }
        public int AddressId { get; set; }
    }
}
