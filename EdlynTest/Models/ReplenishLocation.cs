using System;
using System.Collections.Generic;
using System.Text;

namespace Models
{
    public class ReplenishLocation
    {
        public string WarehouseCode { get; set; }
        public string RoomCode { get; set; }
        public string RackCode { get; set; }
        public string BinLocation { get; set; }
        public DateTime BestBefore { get; set; }
        public string AssignedCatlogCode { get; set; }
        public int ReplenishLevel { get; set; }
        public double UnitsLeft { get; set; }
        public int OrderQuantity { get; set; }
        public int Required { get; set; }
        public int RequiredFull { get; set; }
        public int OrderQuantityLoose { get; set; }
    }
}
