using System;

namespace Models
{
    public class Warehouse
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string CatalogType { get; set; }
        public string Category { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Postcode { get; set; }
        public string Telephone { get; set; }
        public string Contact { get; set; }
        public string CatGroup { get; set; }
        public int CatDisplay { get; set; }
        public string DeleteFlag { get; set; }
        public bool Is3PL { get; set; }
        public string TransitWh { get; set; }
        public string ProductionWh { get; set; }
        public string QualityWh { get; set; }
    }
}
