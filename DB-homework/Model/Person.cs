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

        [Default(0)]
        [CheckConstraint("Age >= 0")]
        public int Age { get; set; }

        [Required]
        public string Phone { get; set; }
    }
}
