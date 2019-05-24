using ServiceStack.DataAnnotations;
using System;

namespace DB_homework.Model
{
    [Alias("Addresses")]
    public class Address
    {
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Text { get; set; }
    }
}
